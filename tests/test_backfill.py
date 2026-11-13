from types import SimpleNamespace

import pytest

import backfill_metrics as bm


def _capture(calls):
    def post(url, **kw):
        calls.append(kw)
        return SimpleNamespace(raise_for_status=lambda: None)
    return post


def test_backfill_import_adds_extra_label_only_when_set(monkeypatch):
    calls = []
    fake = SimpleNamespace(dry_run=False, vm_session=SimpleNamespace(post=_capture(calls)))
    bm.BackfillExporter.send_to_victoria_metrics(fake, ['m 1 1'])
    monkeypatch.setattr(bm, 'VM_EXTRA_LABEL', 'organizer=42')
    bm.BackfillExporter.send_to_victoria_metrics(fake, ['m 1 1'])
    assert calls[0]['params'] is None  # unchanged series identity for the existing org
    assert calls[1]['params'] == {'extra_label': 'organizer=42'}


def test_failed_page_is_not_end_of_event():
    page1 = {'data': [{'ticket_id': 1}], 'pagination': {'next': 'https://x/tickets?after=2026-01-01_1'}}
    responses = iter([page1, None])
    fake = SimpleNamespace(_make_request=lambda *a, **k: next(responses))
    with pytest.raises(RuntimeError, match='page 2'):
        bm.BackfillExporter.fetch_event_tickets(fake, 7)


def test_resold_is_not_refunded():
    ticket = {'ordered_at': '2026-01-01T00:00:00Z', 'event_id': 7,
              'deal_title': 'PASS', 'ticket_status': 'resold'}
    assert bm.BackfillExporter._collect_events_for_ticket(bm.BackfillExporter, ticket, 'ev') == []
