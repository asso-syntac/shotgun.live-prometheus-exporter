import json

import pytest
import requests
from prometheus_client import REGISTRY

import shotgun_exporter as se

TICKETS_PAGE_1 = 'https://api.shotgun.live/tickets?organizer_id=1&after=2026-01-01T00%3A00%3A00Z_1'


@pytest.fixture()
def exporter(tmp_path, monkeypatch):
    monkeypatch.setattr(se.ShotgunExporter, 'DB_FILE', tmp_path / 'test.db')
    monkeypatch.setattr(se.time, 'sleep', lambda s: None)  # instant backoffs
    exp = se.ShotgunExporter()
    exp._recompute_counters()  # isolate the global registry between tests
    return exp


def _resp(status=200, body=None, headers=None):
    r = requests.Response()
    r.status_code = status
    r._content = json.dumps(body if body is not None else {}).encode()
    r.headers.update(headers or {})
    return r


def _ticket(ticket_id, status='valid', title='PASS 3 JOURS', event_id=42):
    return {
        'ticket_id': ticket_id, 'event_id': event_id, 'ticket_status': status,
        'ticket_updated_at': f'2026-01-01T00:00:{ticket_id:02d}Z',
        'ordered_at': '2026-01-01T00:00:00Z',
        'deal_title': title, 'deal_price': 10000, 'deal_channel': 'online',
    }


def _fake_get(monkeypatch, exporter, responses):
    calls = []

    def fake_get(url, params=None, timeout=None):
        calls.append(dict(params or {}))
        return responses[min(len(calls), len(responses)) - 1]

    monkeypatch.setattr(exporter.session, 'get', fake_get)
    return calls


def _sold(labels):
    return REGISTRY.get_sample_value('shotgun_tickets_sold_total', labels)


LABELS = {'event_id': '42', 'event_name': 'Unknown Event', 'ticket_title': 'PASS 3 JOURS'}


def test_make_request_retries_on_429_then_succeeds(exporter, monkeypatch):
    calls = _fake_get(monkeypatch, exporter,
                      [_resp(429), _resp(429), _resp(200, {'data': []})])
    data = exporter._make_request(se.TICKETS_URL, {})
    assert data == {'data': []}
    assert len(calls) == 3


def test_make_request_gives_up_after_persistent_429(exporter, monkeypatch):
    calls = _fake_get(monkeypatch, exporter, [_resp(429)])
    assert exporter._make_request(se.TICKETS_URL, {}) is None
    assert len(calls) == 3  # max_retries


def test_truncated_full_scan_raises_and_keeps_cursor(exporter, monkeypatch):
    page1 = _resp(200, {'data': [_ticket(1)],
                        'pagination': {'next': TICKETS_PAGE_1}})
    _fake_get(monkeypatch, exporter, [page1, _resp(429)])

    with pytest.raises(se.ScanIncomplete) as exc:
        exporter.fetch_all_tickets(full_scan=True)
    assert len(exc.value.tickets) == 1
    # an incomplete FULL scan must not rewind the incremental cursor
    assert exporter._get_last_ticket_after() is None


def test_truncated_incremental_scan_saves_progress_cursor(exporter, monkeypatch):
    page1 = _resp(200, {'data': [_ticket(1)],
                        'pagination': {'next': TICKETS_PAGE_1}})
    _fake_get(monkeypatch, exporter, [page1, _resp(429)])

    with pytest.raises(se.ScanIncomplete):
        exporter.fetch_all_tickets(full_scan=False)
    assert exporter._get_last_ticket_after() == '2026-01-01T00:00:01Z_1'


def test_complete_scan_returns_tickets_and_saves_cursor(exporter, monkeypatch):
    page1 = _resp(200, {'data': [_ticket(1), _ticket(2)], 'pagination': {}})
    _fake_get(monkeypatch, exporter, [page1])

    tickets = exporter.fetch_all_tickets(full_scan=True)
    assert len(tickets) == 2
    assert exporter._get_last_ticket_after() == '2026-01-01T00:00:02Z_2'


def test_recompute_drops_cancelled_from_sold(exporter):
    exporter.process_new_tickets([_ticket(1)])
    exporter._recompute_counters()
    assert _sold(LABELS) == 1

    exporter.process_new_tickets([_ticket(1, status='canceled')])
    exporter._recompute_counters()
    assert _sold(LABELS) is None  # no valid ticket left for this title
    assert REGISTRY.get_sample_value('shotgun_tickets_refunded_total', LABELS) == 1


def test_incomplete_full_scan_upserts_partial_and_retries_next_cycle(exporter, monkeypatch):
    monkeypatch.setattr(
        exporter, 'fetch_all_tickets',
        lambda full_scan: (_ for _ in ()).throw(se.ScanIncomplete([_ticket(7)], 1)))

    assert exporter._run_scan(full_scan=True) is False
    assert exporter._should_do_full_scan() is True  # not marked done
    assert _sold(LABELS) == 1  # partial tickets still upserted


def test_complete_full_scan_marked_done(exporter, monkeypatch):
    monkeypatch.setattr(exporter, 'fetch_all_tickets', lambda full_scan: [_ticket(1)])
    assert exporter._run_scan(full_scan=True) is True
    assert exporter._should_do_full_scan() is False
