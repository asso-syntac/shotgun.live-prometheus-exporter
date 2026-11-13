from types import SimpleNamespace

import backfill_metrics as bm
import reimport_event as ri


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


def test_reimport_delete_scoped_to_extra_label(monkeypatch):
    calls = []
    monkeypatch.setattr(ri.requests, 'post', _capture(calls))
    ri.delete_event_metrics(7, 'ev')
    monkeypatch.setattr(ri, 'VM_EXTRA_LABEL', 'organizer=42')
    ri.delete_event_metrics(7, 'ev')
    n = len(calls) // 2
    assert calls[0]['params']['match[]'] == 'shotgun_tickets_sold_total{event_id="7"}'
    assert calls[n]['params']['match[]'] == 'shotgun_tickets_sold_total{event_id="7",organizer="42"}'
