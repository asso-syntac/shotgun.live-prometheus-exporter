#!/usr/bin/env python3
"""
Backfill historical metrics to VictoriaMetrics with original timestamps.

Processes events one by one to keep memory usage low and supports resuming
after interruption or rate limiting. Each event's tickets are fetched,
cumulative counter values are computed, and sent to VictoriaMetrics.

Progress is tracked in SQLite so the process can be resumed with --resume.
"""

import os
import sys
import json
import time
import re
import sqlite3
import argparse
import requests
from datetime import datetime
from typing import List, Dict, Optional, Tuple
from pathlib import Path
from collections import defaultdict
from urllib.parse import urlparse, parse_qs, unquote
from dotenv import load_dotenv

load_dotenv()

# Configuration
SHOTGUN_TOKEN = os.getenv('SHOTGUN_TOKEN')
SHOTGUN_ORGANIZER_ID = os.getenv('SHOTGUN_ORGANIZER_ID')
INCLUDE_COHOSTED_EVENTS = os.getenv('INCLUDE_COHOSTED_EVENTS', 'false').lower() == 'true'
VICTORIA_METRICS_URL = os.getenv('VICTORIA_METRICS_URL', 'http://victoria-metrics:8428')

TICKETS_URL = "https://api.shotgun.live/tickets"
EVENTS_URL = f"https://smartboard-api.shotgun.live/api/shotgun/organizers/{SHOTGUN_ORGANIZER_ID}/events"

# Rate limiting: 200 calls/min = 300ms between calls
API_RATE_LIMIT_DELAY = 0.3

DB_FILE = Path('/data/shotgun_tickets.db')


class BackfillExporter:
    def __init__(self, dry_run: bool = False, batch_size: int = 1000, resume: bool = False):
        if not SHOTGUN_TOKEN:
            raise ValueError("SHOTGUN_TOKEN must be defined in .env file")
        if not SHOTGUN_ORGANIZER_ID:
            raise ValueError("SHOTGUN_ORGANIZER_ID must be defined in .env file")

        self.session = requests.Session()
        self.session.headers['Authorization'] = f'Bearer {SHOTGUN_TOKEN}'
        self._last_api_call = 0
        self.dry_run = dry_run
        self.batch_size = batch_size
        self.resume = resume
        self.events_cache: Dict[int, str] = {}

        # Init database
        DB_FILE.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _init_db(self):
        """Ensure all required tables exist"""
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS tickets (
                ticket_id INTEGER PRIMARY KEY,
                event_id INTEGER NOT NULL,
                ticket_status TEXT NOT NULL,
                ticket_scanned_at TEXT,
                ticket_updated_at TEXT NOT NULL,
                ticket_canceled_at TEXT,
                deal_id INTEGER,
                deal_title TEXT,
                deal_sub_category TEXT,
                deal_channel TEXT,
                deal_visibilities TEXT,
                deal_price INTEGER,
                deal_service_fee INTEGER,
                deal_user_service_fee INTEGER,
                currency TEXT,
                payment_method TEXT,
                utm_source TEXT,
                utm_medium TEXT,
                order_id INTEGER,
                ordered_at TEXT NOT NULL,
                ticket_data TEXT,
                first_seen_at TEXT NOT NULL,
                last_updated_at TEXT NOT NULL
            )
        ''')

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS ticket_status_changes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticket_id INTEGER NOT NULL,
                old_status TEXT,
                new_status TEXT NOT NULL,
                changed_at TEXT NOT NULL,
                FOREIGN KEY (ticket_id) REFERENCES tickets (ticket_id)
            )
        ''')

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS exporter_state (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
        ''')

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS events_cache (
                event_id INTEGER PRIMARY KEY,
                event_name TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
        ''')

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS backfill_progress (
                event_id INTEGER PRIMARY KEY,
                status TEXT NOT NULL DEFAULT 'pending',
                tickets_count INTEGER DEFAULT 0,
                metrics_sent INTEGER DEFAULT 0,
                updated_at TEXT NOT NULL
            )
        ''')

        cursor.execute('CREATE INDEX IF NOT EXISTS idx_event_id ON tickets(event_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_status ON tickets(ticket_status)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_ticket_updated_at ON tickets(ticket_updated_at)')

        conn.commit()
        conn.close()

    # ── API helpers ──────────────────────────────────────────────────────

    def _rate_limit(self):
        now = time.time()
        elapsed = now - self._last_api_call
        if elapsed < API_RATE_LIMIT_DELAY:
            time.sleep(API_RATE_LIMIT_DELAY - elapsed)
        self._last_api_call = time.time()

    def _make_request(self, url: str, params: Optional[Dict] = None,
                      use_token: bool = True, max_retries: int = 10) -> Optional[Dict]:
        """Make HTTP request with rate limiting and retry on 429/5xx."""
        for attempt in range(max_retries):
            self._rate_limit()

            try:
                if use_token:
                    full_params = params.copy() if params else {}
                else:
                    full_params = {'key': SHOTGUN_TOKEN}
                    if params:
                        full_params.update(params)

                response = self.session.get(url, params=full_params, timeout=120)

                if response.status_code == 429:
                    retry_after = int(response.headers.get('Retry-After', 60))
                    print(f"    Rate limited (429), waiting {retry_after}s ({attempt + 1}/{max_retries})...")
                    time.sleep(retry_after)
                    continue

                response.raise_for_status()
                return response.json()

            except requests.exceptions.RequestException as e:
                status = getattr(getattr(e, 'response', None), 'status_code', None)
                if status and 500 <= status < 600 and attempt < max_retries - 1:
                    wait = min(2 ** attempt, 120)
                    print(f"    Server error {status}, retrying in {wait}s ({attempt + 1}/{max_retries})...")
                    time.sleep(wait)
                    continue
                print(f"  Error during request: {e}")
                return None

        print(f"  Failed after {max_retries} attempts")
        return None

    # ── Events ───────────────────────────────────────────────────────────

    def fetch_events(self) -> List[Dict]:
        """Fetch all events and populate cache"""
        print("Fetching events...")

        future_data = self._make_request(EVENTS_URL, use_token=False)
        future = future_data.get('data', []) if future_data else []

        past_data = self._make_request(EVENTS_URL, {'past_events': 'true', 'limit': 100}, use_token=False)
        past = past_data.get('data', []) if past_data else []

        all_events = future + past
        print(f"Fetched {len(all_events)} events")

        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        now = datetime.now().isoformat()

        for event in all_events:
            eid = event.get('id')
            ename = event.get('name', 'Unknown Event')
            if eid:
                self.events_cache[eid] = ename
                cursor.execute(
                    'INSERT OR REPLACE INTO events_cache (event_id, event_name, updated_at) VALUES (?, ?, ?)',
                    (eid, ename, now)
                )

        conn.commit()
        conn.close()
        return all_events

    def _get_event_name(self, event_id: int) -> str:
        return self.events_cache.get(event_id, 'Unknown Event')

    # ── Tickets (per event) ──────────────────────────────────────────────

    def fetch_event_tickets(self, event_id: int) -> List[Dict]:
        """Fetch all tickets for a single event"""
        all_tickets = []
        params = {
            'organizer_id': SHOTGUN_ORGANIZER_ID,
            'event_id': str(event_id),
        }
        if INCLUDE_COHOSTED_EVENTS:
            params['include_cohosted_events'] = 'true'

        page = 0
        while True:
            data = self._make_request(TICKETS_URL, params, use_token=True)
            if not data:
                break

            tickets = data.get('data', [])
            if not tickets:
                break

            all_tickets.extend(tickets)
            page += 1

            if page % 10 == 0:
                print(f"    Page {page}: {len(all_tickets)} tickets so far...")

            next_url = data.get('pagination', {}).get('next')
            if not next_url:
                break

            try:
                parsed = urlparse(next_url)
                after_values = parse_qs(parsed.query).get('after', [])
                if after_values:
                    params['after'] = unquote(after_values[0])
                else:
                    break
            except Exception:
                break

        return all_tickets

    # ── Metric generation ────────────────────────────────────────────────

    @staticmethod
    def _normalize_ticket_title(ticket: Dict) -> str:
        title = ticket.get('deal_title', 'Unknown Ticket')
        if re.match(r'^\d{3,}', str(title)):
            sub = ticket.get('deal_sub_category')
            if sub:
                return sub
        return title if title else 'Unknown Ticket'

    @staticmethod
    def _get_timestamp_ms(iso_string: str) -> Optional[int]:
        if not iso_string:
            return None
        try:
            if iso_string.endswith('Z'):
                dt = datetime.fromisoformat(iso_string.replace('Z', '+00:00'))
            else:
                dt = datetime.fromisoformat(iso_string)
            return int(dt.timestamp() * 1000)
        except Exception:
            return None

    @staticmethod
    def _format_line(metric: str, labels: Dict[str, str], value: float, ts_ms: int) -> str:
        escaped = {k: str(v).replace('\\', '\\\\').replace('"', '\\"').replace('\n', '\\n')
                   for k, v in labels.items()}
        lbl = ','.join(f'{k}="{v}"' for k, v in escaped.items())
        return f"{metric}{{{lbl}}} {value} {ts_ms}"

    def _collect_events_for_ticket(self, ticket: Dict, event_name: str) -> List[Tuple[str, str, float, int]]:
        """Return list of (metric, labels_key, increment, timestamp_ms) for one ticket."""
        ordered_at = ticket.get('ordered_at')
        if not ordered_at:
            return []
        ts = self._get_timestamp_ms(ordered_at)
        if not ts:
            return []

        eid = str(ticket.get('event_id'))
        title = self._normalize_ticket_title(ticket)
        status = ticket.get('ticket_status', 'unknown')
        results = []

        # Escape label values for Prometheus exposition format
        def esc(v: str) -> str:
            return v.replace('\\', '\\\\').replace('"', '\\"').replace('\n', '\\n')

        ename = esc(event_name)
        etitle = esc(title)

        base = f'event_id="{eid}",event_name="{ename}",ticket_title="{etitle}"'

        if status == 'valid':
            results.append(('shotgun_tickets_sold_total', base, 1, ts))

            price = (ticket.get('deal_price') or 0) / 100.0
            results.append(('shotgun_tickets_revenue_euros_total', base, price, ts))

            ch = esc(ticket.get('deal_channel') or 'unknown')
            results.append(('shotgun_tickets_by_channel_total',
                            f'event_id="{eid}",event_name="{ename}",channel="{ch}"', 1, ts))

            pm = esc(ticket.get('payment_method') or 'unknown')
            results.append(('shotgun_tickets_by_payment_method_total',
                            f'event_id="{eid}",event_name="{ename}",payment_method="{pm}"', 1, ts))

            us = esc(ticket.get('utm_source') or 'unknown')
            results.append(('shotgun_tickets_by_utm_source_total',
                            f'event_id="{eid}",event_name="{ename}",utm_source="{us}"', 1, ts))

            um = esc(ticket.get('utm_medium') or 'unknown')
            results.append(('shotgun_tickets_by_utm_medium_total',
                            f'event_id="{eid}",event_name="{ename}",utm_medium="{um}"', 1, ts))

            for vis in (ticket.get('deal_visibilities') or []):
                results.append(('shotgun_tickets_by_visibility_total',
                                f'event_id="{eid}",event_name="{ename}",visibility="{esc(vis)}"', 1, ts))

            sf = ticket.get('deal_service_fee') or 0
            if sf:
                results.append(('shotgun_tickets_fees_euros_total',
                                f'event_id="{eid}",event_name="{ename}",fee_type="service_fee"',
                                sf / 100.0, ts))

            usf = ticket.get('deal_user_service_fee') or 0
            if usf:
                results.append(('shotgun_tickets_fees_euros_total',
                                f'event_id="{eid}",event_name="{ename}",fee_type="user_service_fee"',
                                usf / 100.0, ts))

        elif status in ['refunded', 'canceled', 'resold']:
            cancel_ts = self._get_timestamp_ms(ticket.get('ticket_canceled_at') or ordered_at)
            if cancel_ts:
                results.append(('shotgun_tickets_refunded_total', base, 1, cancel_ts))

        scan_at = ticket.get('ticket_scanned_at')
        if scan_at:
            scan_ts = self._get_timestamp_ms(scan_at)
            if scan_ts:
                results.append(('shotgun_tickets_scanned_total',
                                f'event_id="{eid}",event_name="{ename}"', 1, scan_ts))

        return results

    # Interval (ms) between fill-forward points to keep series continuous.
    # Must match or be smaller than the scrape interval (5m) so that
    # range queries at any step never see a gap.
    FILL_INTERVAL_MS = 5 * 60 * 1000

    def build_cumulative_lines(self, tickets: List[Dict], event_name: str) -> List[str]:
        """Build cumulative Prometheus lines for a list of tickets (single event).

        After computing cumulative values, emits fill-forward points at regular
        intervals between real data points AND after the last point of each
        series up to the event's last activity timestamp. This ensures all
        series for a given event span the same time range (e.g. "Tarif Early"
        that ended early still shows its final value until the event is over).
        """
        # Collect all metric events
        raw_events = []
        for ticket in tickets:
            raw_events.extend(self._collect_events_for_ticket(ticket, event_name))

        if not raw_events:
            return []

        # Sort by timestamp
        raw_events.sort(key=lambda e: e[3])

        # Find the latest timestamp across ALL series of this event.
        # This is the "event end" — all series will be filled up to this point.
        event_end_ms = max(e[3] for e in raw_events)

        # Build cumulative values at each real event timestamp
        cumulative: Dict[str, float] = defaultdict(float)
        # Track per series: list of (ts_ms, cumulative_value)
        series_points: Dict[str, List[Tuple[int, float]]] = defaultdict(list)

        for metric, labels_key, increment, ts_ms in raw_events:
            key = f"{metric}|{labels_key}"
            cumulative[key] += increment
            series_points[key].append((ts_ms, cumulative[key]))

        lines = []

        for key, points in series_points.items():
            metric, labels_key = key.split('|', 1)

            prev_ts = None
            prev_val = None

            for ts_ms, val in points:
                # Fill gap between previous point and this one
                if prev_ts is not None:
                    fill_ts = prev_ts + self.FILL_INTERVAL_MS
                    while fill_ts < ts_ms:
                        lines.append(f"{metric}{{{labels_key}}} {prev_val} {fill_ts}")
                        fill_ts += self.FILL_INTERVAL_MS

                # Emit the real point
                lines.append(f"{metric}{{{labels_key}}} {val} {ts_ms}")
                prev_ts = ts_ms
                prev_val = val

            # Fill forward from last point of this series to event end
            if prev_ts is not None and prev_ts < event_end_ms:
                fill_ts = prev_ts + self.FILL_INTERVAL_MS
                while fill_ts <= event_end_ms:
                    lines.append(f"{metric}{{{labels_key}}} {prev_val} {fill_ts}")
                    fill_ts += self.FILL_INTERVAL_MS

        # Sort all lines by timestamp for ordered ingestion
        lines.sort(key=lambda l: int(l.rsplit(' ', 1)[1]))

        return lines

    # ── VictoriaMetrics ──────────────────────────────────────────────────

    def send_to_victoria_metrics(self, lines: List[str]) -> bool:
        if not lines:
            return True
        if self.dry_run:
            return True

        try:
            url = f"{VICTORIA_METRICS_URL}/api/v1/import/prometheus"
            response = requests.post(
                url,
                data='\n'.join(lines).encode('utf-8'),
                headers={'Content-Type': 'text/plain'},
                timeout=60
            )
            response.raise_for_status()
            return True
        except Exception as e:
            print(f"    Error sending to VictoriaMetrics: {e}")
            return False

    def send_lines_batched(self, lines: List[str]) -> int:
        """Send lines in batches, return number sent."""
        sent = 0
        for i in range(0, len(lines), self.batch_size):
            batch = lines[i:i + self.batch_size]
            if self.send_to_victoria_metrics(batch):
                sent += len(batch)
            else:
                print(f"    Failed batch at offset {i}")
        return sent

    # ── DB helpers ───────────────────────────────────────────────────────

    def save_tickets_to_db(self, tickets: List[Dict]):
        """Save tickets for one event to database"""
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        now = datetime.now().isoformat()

        for ticket in tickets:
            tid = ticket.get('ticket_id')
            if not tid:
                continue

            cursor.execute('''
                INSERT OR REPLACE INTO tickets (
                    ticket_id, event_id, ticket_status, ticket_scanned_at, ticket_updated_at,
                    ticket_canceled_at, deal_id, deal_title, deal_sub_category, deal_channel,
                    deal_visibilities, deal_price, deal_service_fee, deal_user_service_fee,
                    currency, payment_method, utm_source, utm_medium, order_id, ordered_at,
                    ticket_data, first_seen_at, last_updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                tid,
                ticket.get('event_id'),
                ticket.get('ticket_status'),
                ticket.get('ticket_scanned_at'),
                ticket.get('ticket_updated_at'),
                ticket.get('ticket_canceled_at'),
                ticket.get('deal_id'),
                ticket.get('deal_title'),
                ticket.get('deal_sub_category'),
                ticket.get('deal_channel'),
                json.dumps(ticket.get('deal_visibilities', [])),
                ticket.get('deal_price'),
                ticket.get('deal_service_fee'),
                ticket.get('deal_user_service_fee'),
                ticket.get('currency'),
                ticket.get('payment_method'),
                ticket.get('utm_source'),
                ticket.get('utm_medium'),
                ticket.get('order_id'),
                ticket.get('ordered_at'),
                json.dumps(ticket),
                now, now
            ))

        conn.commit()
        conn.close()

    def _mark_event_progress(self, event_id: int, status: str,
                             tickets_count: int = 0, metrics_sent: int = 0):
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute('''
            INSERT OR REPLACE INTO backfill_progress
                (event_id, status, tickets_count, metrics_sent, updated_at)
            VALUES (?, ?, ?, ?, ?)
        ''', (event_id, status, tickets_count, metrics_sent, datetime.now().isoformat()))
        conn.commit()
        conn.close()

    def _get_completed_events(self) -> set:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute("SELECT event_id FROM backfill_progress WHERE status = 'done'")
        result = {row[0] for row in cursor.fetchall()}
        conn.close()
        return result

    # ── Main process ─────────────────────────────────────────────────────

    def process_event(self, event_id: int, event_name: str) -> bool:
        """Fetch, compute, and send metrics for a single event. Returns True on success."""
        print(f"\n  [{event_id}] {event_name}")
        self._mark_event_progress(event_id, 'fetching')

        # Fetch tickets for this event
        tickets = self.fetch_event_tickets(event_id)
        print(f"    {len(tickets)} tickets fetched")

        if not tickets:
            self._mark_event_progress(event_id, 'done', 0, 0)
            return True

        # Build cumulative metric lines
        lines = self.build_cumulative_lines(tickets, event_name)
        print(f"    {len(lines)} cumulative metric lines generated")

        # Send to VictoriaMetrics
        self._mark_event_progress(event_id, 'sending', len(tickets), 0)
        sent = self.send_lines_batched(lines)
        print(f"    {sent}/{len(lines)} metric lines sent")

        # Save tickets to DB
        if not self.dry_run:
            self.save_tickets_to_db(tickets)

        self._mark_event_progress(event_id, 'done', len(tickets), sent)
        return True

    def run(self):
        """Main backfill process: event by event with resume support."""
        print("=" * 80)
        print("Shotgun Metrics Backfill (event-by-event)")
        print("=" * 80)

        if self.dry_run:
            print("[DRY RUN MODE]")
        if self.resume:
            print("[RESUME MODE - skipping already completed events]")

        # Step 1: Fetch events
        all_events = self.fetch_events()
        if not all_events:
            print("No events found")
            return

        event_list = [(e['id'], e.get('name', 'Unknown')) for e in all_events if e.get('id')]
        print(f"\n{len(event_list)} events to process")

        # Filter already completed events in resume mode
        completed = self._get_completed_events() if self.resume else set()
        if completed:
            print(f"Skipping {len(completed)} already completed events")

        # Step 2: Process each event
        success = 0
        skipped = 0
        failed = 0

        for i, (eid, ename) in enumerate(event_list):
            if eid in completed:
                skipped += 1
                continue

            print(f"\n[{i + 1}/{len(event_list)}]", end="")
            try:
                if self.process_event(eid, ename):
                    success += 1
                else:
                    failed += 1
            except KeyboardInterrupt:
                print(f"\n\nInterrupted! Progress saved. Resume with --resume")
                self._mark_event_progress(eid, 'interrupted')
                break
            except Exception as e:
                print(f"    ERROR: {e}")
                self._mark_event_progress(eid, 'error')
                failed += 1

        # Step 3: Update exporter state
        if not self.dry_run and success > 0:
            conn = sqlite3.connect(DB_FILE)
            cursor = conn.cursor()
            now = datetime.now().isoformat()
            cursor.execute('''
                INSERT OR REPLACE INTO exporter_state (key, value, updated_at)
                VALUES ('last_full_scan', ?, ?)
            ''', (now, now))

            # Save last_ticket_after from the most recent ticket
            cursor.execute('''
                SELECT ticket_updated_at, ticket_id FROM tickets
                ORDER BY ticket_updated_at DESC, ticket_id DESC LIMIT 1
            ''')
            row = cursor.fetchone()
            if row:
                cursor.execute('''
                    INSERT OR REPLACE INTO exporter_state (key, value, updated_at)
                    VALUES ('last_ticket_after', ?, ?)
                ''', (f"{row[0]}_{row[1]}", now))

            conn.commit()
            conn.close()

        # Summary
        print("\n" + "=" * 80)
        print(f"Backfill complete!")
        print(f"  Success: {success} events")
        if skipped:
            print(f"  Skipped: {skipped} events (already done)")
        if failed:
            print(f"  Failed:  {failed} events")
        print(f"  Total:   {len(event_list)} events")
        if failed:
            print(f"\nRe-run with --resume to retry failed events")
        print("=" * 80)


def main():
    parser = argparse.ArgumentParser(
        description='Backfill historical Shotgun metrics to VictoriaMetrics',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  # Dry run (no data written)
  python backfill_metrics.py --dry-run

  # Full backfill (all events)
  python backfill_metrics.py

  # Resume after interruption or rate limiting
  python backfill_metrics.py --resume

  # With custom batch size
  python backfill_metrics.py --batch-size 500
        '''
    )

    parser.add_argument('--dry-run', action='store_true',
                        help='Show what would be done without writing data')
    parser.add_argument('--resume', action='store_true',
                        help='Skip events already completed in a previous run')
    parser.add_argument('--batch-size', type=int, default=1000,
                        help='Number of metric lines per batch (default: 1000)')

    args = parser.parse_args()

    try:
        exporter = BackfillExporter(
            dry_run=args.dry_run,
            batch_size=args.batch_size,
            resume=args.resume
        )
        exporter.run()
    except ValueError as e:
        print(f"Configuration error: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        print("\nInterrupted by user. Re-run with --resume to continue.")
        sys.exit(1)


if __name__ == '__main__':
    main()
