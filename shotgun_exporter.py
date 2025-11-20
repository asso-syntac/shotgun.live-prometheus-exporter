#!/usr/bin/env python3

import os
import time
import logging
import json
import sqlite3
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from pathlib import Path
import requests
from prometheus_client import start_http_server, Gauge, Counter, Info
from dotenv import load_dotenv

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

load_dotenv()

SHOTGUN_API_KEY = os.getenv('SHOTGUN_API_KEY')
SHOTGUN_ORGANIZER_ID = os.getenv('SHOTGUN_ORGANIZER_ID')
EXPORTER_PORT = int(os.getenv('EXPORTER_PORT', '9090'))
SCRAPE_INTERVAL = int(os.getenv('SCRAPE_INTERVAL', '60'))
INCLUDE_COHOSTED_EVENTS = os.getenv('INCLUDE_COHOSTED_EVENTS', 'false').lower() == 'true'
FULL_SCAN_INTERVAL = int(os.getenv('FULL_SCAN_INTERVAL', '86400'))
EVENTS_FETCH_INTERVAL = int(os.getenv('EVENTS_FETCH_INTERVAL', '3600'))
RECENT_SCAN_INTERVAL = int(os.getenv('RECENT_SCAN_INTERVAL', '3600'))

BASE_URL = "https://smartboard-api.shotgun.live/api/shotgun"
TICKETS_URL = f"{BASE_URL}/tickets/sold"
EVENTS_URL = f"{BASE_URL}/organizers/{SHOTGUN_ORGANIZER_ID}/events"

tickets_sold_total = Counter(
    'shotgun_tickets_sold_total',
    'Total number of tickets sold',
    ['event_id', 'event_name', 'ticket_title']
)

tickets_revenue_total = Counter(
    'shotgun_tickets_revenue_euros_total',
    'Total revenue from ticket sales in euros',
    ['event_id', 'event_name', 'ticket_title']
)

tickets_by_channel_total = Counter(
    'shotgun_tickets_by_channel_total',
    'Number of tickets sold by channel',
    ['event_id', 'event_name', 'channel']
)

tickets_refunded_total = Counter(
    'shotgun_tickets_refunded_total',
    'Number of tickets refunded',
    ['event_id', 'event_name', 'ticket_title']
)

tickets_scanned_total = Counter(
    'shotgun_tickets_scanned_total',
    'Number of tickets scanned',
    ['event_id', 'event_name']
)

events_total = Gauge(
    'shotgun_events_total',
    'Total number of events',
    ['status']
)

event_tickets_left = Gauge(
    'shotgun_event_tickets_left',
    'Number of tickets left for an event',
    ['event_id', 'event_name']
)

event_info = Info(
    'shotgun_event',
    'Information about Shotgun events'
)

api_requests_total = Counter(
    'shotgun_api_requests_total',
    'Total number of requests to Shotgun API',
    ['endpoint', 'status']
)

last_scrape_timestamp = Gauge(
    'shotgun_last_scrape_timestamp',
    'Timestamp of last successful scrape'
)


class ShotgunExporter:
    DB_FILE = Path('/data/shotgun_tickets.db')

    def __init__(self):
        if not SHOTGUN_API_KEY:
            raise ValueError("SHOTGUN_API_KEY must be defined in .env file")
        if not SHOTGUN_ORGANIZER_ID:
            raise ValueError("SHOTGUN_ORGANIZER_ID must be defined in .env file")

        self.session = requests.Session()
        self.session.params = {'key': SHOTGUN_API_KEY}

        self._init_database()

    def _init_database(self):
        try:
            conn = sqlite3.connect(self.DB_FILE)
            cursor = conn.cursor()

            cursor.execute('''
                CREATE TABLE IF NOT EXISTS tickets (
                    ticket_id TEXT PRIMARY KEY,
                    event_id TEXT NOT NULL,
                    event_name TEXT,
                    ticket_title TEXT,
                    ticket_status TEXT,
                    ticket_price REAL,
                    channel TEXT,
                    ticket_redeemed_at TEXT,
                    ticket_data TEXT,
                    first_seen_at TEXT NOT NULL,
                    last_updated_at TEXT NOT NULL
                )
            ''')

            cursor.execute('''
                CREATE TABLE IF NOT EXISTS ticket_status_changes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ticket_id TEXT NOT NULL,
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

            cursor.execute('CREATE INDEX IF NOT EXISTS idx_event_id ON tickets(event_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_status ON tickets(ticket_status)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_status_changes_ticket ON ticket_status_changes(ticket_id)')

            conn.commit()

            cursor.execute('SELECT COUNT(*) FROM tickets')
            count = cursor.fetchone()[0]
            logger.info(f"Database initialized: {count} tickets in database")

            if count > 0:
                self._restore_counters_from_db(conn)

            conn.close()
        except Exception as e:
            logger.error(f"Error initializing database: {e}")
            raise

    def _restore_counters_from_db(self, conn: sqlite3.Connection):
        cursor = conn.cursor()

        cursor.execute('''
            SELECT event_id, event_name, ticket_title, COUNT(*), SUM(ticket_price)
            FROM tickets
            WHERE ticket_status = 'valid'
            GROUP BY event_id, event_name, ticket_title
        ''')
        for row in cursor.fetchall():
            event_id, event_name, ticket_title, count, total_revenue = row
            tickets_sold_total.labels(
                event_id=event_id,
                event_name=event_name,
                ticket_title=ticket_title
            ).inc(count)
            tickets_revenue_total.labels(
                event_id=event_id,
                event_name=event_name,
                ticket_title=ticket_title
            ).inc(total_revenue or 0)

        cursor.execute('''
            SELECT event_id, event_name, channel, COUNT(*)
            FROM tickets
            WHERE ticket_status = 'valid'
            GROUP BY event_id, event_name, channel
        ''')
        for row in cursor.fetchall():
            event_id, event_name, channel, count = row
            tickets_by_channel_total.labels(
                event_id=event_id,
                event_name=event_name,
                channel=channel
            ).inc(count)

        cursor.execute('''
            SELECT event_id, event_name, ticket_title, COUNT(*)
            FROM tickets
            WHERE ticket_status IN ('refunded', 'canceled')
            GROUP BY event_id, event_name, ticket_title
        ''')
        for row in cursor.fetchall():
            event_id, event_name, ticket_title, count = row
            tickets_refunded_total.labels(
                event_id=event_id,
                event_name=event_name,
                ticket_title=ticket_title
            ).inc(count)

        cursor.execute('''
            SELECT event_id, event_name, COUNT(*)
            FROM tickets
            WHERE ticket_redeemed_at IS NOT NULL
            GROUP BY event_id, event_name
        ''')
        for row in cursor.fetchall():
            event_id, event_name, count = row
            tickets_scanned_total.labels(
                event_id=event_id,
                event_name=event_name
            ).inc(count)

        logger.info("Counters restored from database")

    def _make_request(self, url: str, params: Optional[Dict] = None) -> Optional[Dict]:
        try:
            full_params = {'key': SHOTGUN_API_KEY}
            if params:
                full_params.update(params)

            response = self.session.get(url, params=full_params, timeout=120)

            if response.status_code != 200:
                logger.error(f"API error - Status: {response.status_code}")
                logger.error(f"URL: {url}")
                logger.error(f"Response: {response.text[:500]}")

            response.raise_for_status()

            api_requests_total.labels(endpoint=url.split('/')[-1], status='success').inc()
            return response.json()

        except requests.exceptions.Timeout as e:
            logger.error(f"Timeout during request to {url}: {e}")
            api_requests_total.labels(endpoint=url.split('/')[-1], status='timeout').inc()
            return None
        except requests.exceptions.RequestException as e:
            logger.error(f"Error during request to {url}: {e}")
            api_requests_total.labels(endpoint=url.split('/')[-1], status='error').inc()
            return None

    def _should_do_full_scan(self) -> bool:
        try:
            conn = sqlite3.connect(self.DB_FILE)
            cursor = conn.cursor()
            cursor.execute("SELECT value FROM exporter_state WHERE key = 'last_full_scan'")
            row = cursor.fetchone()
            conn.close()

            if not row:
                return True

            last_scan = datetime.fromisoformat(row[0])
            time_since_scan = (datetime.now() - last_scan).total_seconds()
            return time_since_scan >= FULL_SCAN_INTERVAL
        except Exception as e:
            logger.warning(f"Error reading last full scan time: {e}")
            return True

    def _mark_full_scan_done(self):
        try:
            conn = sqlite3.connect(self.DB_FILE)
            cursor = conn.cursor()
            now = datetime.now().isoformat()
            cursor.execute('''
                INSERT OR REPLACE INTO exporter_state (key, value, updated_at)
                VALUES ('last_full_scan', ?, ?)
            ''', (now, now))
            conn.commit()
            conn.close()
        except Exception as e:
            logger.error(f"Error writing full scan timestamp: {e}")

    def _should_fetch_events(self) -> bool:
        try:
            conn = sqlite3.connect(self.DB_FILE)
            cursor = conn.cursor()
            cursor.execute("SELECT value FROM exporter_state WHERE key = 'last_events_fetch'")
            row = cursor.fetchone()
            conn.close()

            if not row:
                return True

            last_fetch = datetime.fromisoformat(row[0])
            time_since_fetch = (datetime.now() - last_fetch).total_seconds()
            return time_since_fetch >= EVENTS_FETCH_INTERVAL
        except Exception as e:
            logger.warning(f"Error reading last events fetch time: {e}")
            return True

    def _mark_events_fetched(self):
        try:
            conn = sqlite3.connect(self.DB_FILE)
            cursor = conn.cursor()
            now = datetime.now().isoformat()
            cursor.execute('''
                INSERT OR REPLACE INTO exporter_state (key, value, updated_at)
                VALUES ('last_events_fetch', ?, ?)
            ''', (now, now))
            conn.commit()
            conn.close()
        except Exception as e:
            logger.error(f"Error writing events fetch timestamp: {e}")

    def _should_do_recent_scan(self) -> bool:
        try:
            conn = sqlite3.connect(self.DB_FILE)
            cursor = conn.cursor()
            cursor.execute("SELECT value FROM exporter_state WHERE key = 'last_recent_scan'")
            row = cursor.fetchone()
            conn.close()

            if not row:
                return True

            last_scan = datetime.fromisoformat(row[0])
            time_since_scan = (datetime.now() - last_scan).total_seconds()
            return time_since_scan >= RECENT_SCAN_INTERVAL
        except Exception as e:
            logger.warning(f"Error reading last recent scan time: {e}")
            return True

    def _mark_recent_scan_done(self):
        try:
            conn = sqlite3.connect(self.DB_FILE)
            cursor = conn.cursor()
            now = datetime.now().isoformat()
            cursor.execute('''
                INSERT OR REPLACE INTO exporter_state (key, value, updated_at)
                VALUES ('last_recent_scan', ?, ?)
            ''', (now, now))
            conn.commit()
            conn.close()
        except Exception as e:
            logger.error(f"Error writing recent scan timestamp: {e}")

    def fetch_all_tickets(self, full_scan: bool = False, recent_only: bool = False) -> List[Dict]:
        all_tickets = []

        params = {
            'organizer_id': SHOTGUN_ORGANIZER_ID,
            'cursor': ''
        }

        if INCLUDE_COHOSTED_EVENTS:
            params['include_cohosted_events'] = 'true'

        scan_mode = "recent scan (24h)" if recent_only else ("full scan" if full_scan else "incremental")
        if INCLUDE_COHOSTED_EVENTS:
            logger.info(f"Fetching tickets ({scan_mode}, including co-hosted events)...")
        else:
            logger.info(f"Fetching tickets ({scan_mode})...")

        page_count = 0
        seen_known_tickets = 0
        conn = sqlite3.connect(self.DB_FILE)
        cutoff_time = datetime.now() - timedelta(hours=24)

        while True:
            try:
                data = self._make_request(TICKETS_URL, params)
                if not data:
                    logger.warning(f"No data received at page {page_count + 1}, stopping pagination")
                    break

                tickets = data.get('data', [])
                if not tickets:
                    logger.info(f"No tickets at page {page_count + 1}, end of pagination")
                    break

                all_tickets.extend(tickets)
                page_count += 1

                pagination_info = data.get('pagination', {})
                total_results = pagination_info.get('totalResults', '?')
                logger.info(f"Page {page_count}: {len(tickets)} tickets fetched (total: {len(all_tickets)}/{total_results})")

                # For recent_only mode, stop when we hit tickets older than 24h
                if recent_only:
                    old_tickets_in_page = 0
                    for ticket in tickets:
                        # Use 'ordered_at' which is the purchase date
                        ordered_at_str = ticket.get('ordered_at')
                        if ordered_at_str:
                            try:
                                ordered_at = datetime.fromisoformat(ordered_at_str.replace('Z', '+00:00'))
                                if ordered_at.replace(tzinfo=None) < cutoff_time:
                                    old_tickets_in_page += 1
                            except Exception as e:
                                logger.debug(f"Could not parse date '{ordered_at_str}': {e}")
                                pass

                    if old_tickets_in_page >= len(tickets) * 0.8:  # If 80% of page is >24h old
                        logger.info(f"Recent scan: {old_tickets_in_page}/{len(tickets)} tickets older than 24h, stopping pagination")
                        conn.close()
                        logger.info(f"Total: {len(all_tickets)} tickets fetched in {page_count} page(s)")
                        return all_tickets

                # For incremental mode, stop when all tickets in page are known
                if not full_scan and not recent_only:
                    for ticket in tickets:
                        ticket_id = ticket.get('ticket_id')
                        if ticket_id and self._get_ticket_from_db(conn, ticket_id):
                            seen_known_tickets += 1

                    if seen_known_tickets >= len(tickets):
                        logger.info(f"Incremental scan: all tickets in page already known, stopping pagination")
                        conn.close()
                        logger.info(f"Total: {len(all_tickets)} tickets fetched in {page_count} page(s)")
                        return all_tickets

                next_url = pagination_info.get('next')
                if not next_url:
                    logger.info("No next page, end of pagination")
                    break

                if 'cursor=' in next_url:
                    cursor_part = next_url.split('cursor=')[1]
                    cursor = cursor_part.split('&')[0] if '&' in cursor_part else cursor_part
                    params['cursor'] = cursor
                    logger.debug(f"Cursor for next page: {cursor[:50]}...")
                else:
                    break

            except Exception as e:
                logger.error(f"Error fetching page {page_count + 1}: {e}")
                break

        conn.close()
        logger.info(f"Total: {len(all_tickets)} tickets fetched in {page_count} page(s)")
        return all_tickets

    def fetch_events(self) -> List[Dict]:
        logger.info("Fetching events...")

        future_events_data = self._make_request(EVENTS_URL)
        future_events = future_events_data.get('data', []) if future_events_data else []

        past_events_data = self._make_request(EVENTS_URL, {'past_events': 'true', 'limit': 100})
        past_events = past_events_data.get('data', []) if past_events_data else []

        all_events = future_events + past_events
        logger.info(f"Total: {len(all_events)} events fetched")

        return all_events

    def _normalize_ticket_title(self, ticket: Dict) -> str:
        import re
        ticket_title = ticket.get('ticket_title', 'Unknown Ticket')

        if re.match(r'^\d{3,}', str(ticket_title)):
            ticket_sub_category = ticket.get('ticket_sub_category')
            if ticket_sub_category:
                return ticket_sub_category

        return ticket_title if ticket_title else 'Unknown Ticket'

    def _filter_personal_data(self, ticket: Dict) -> Dict:
        personal_fields = [
            'buyer_email',
            'buyer_phone',
            'buyer_first_name',
            'buyer_last_name',
            'buyer_gender',
            'buyer_birthday',
            'buyer_company_name',
            'buyer_newsletter_optin'
        ]

        filtered_ticket = ticket.copy()
        for field in personal_fields:
            if field in filtered_ticket:
                del filtered_ticket[field]

        return filtered_ticket

    def _get_ticket_from_db(self, conn: sqlite3.Connection, ticket_id: str) -> Optional[Dict]:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT ticket_id, event_id, event_name, ticket_title, ticket_status,
                   ticket_price, channel, ticket_redeemed_at, ticket_data
            FROM tickets WHERE ticket_id = ?
        ''', (ticket_id,))
        row = cursor.fetchone()
        if row:
            return {
                'ticket_id': row[0],
                'event_id': row[1],
                'event_name': row[2],
                'ticket_title': row[3],
                'ticket_status': row[4],
                'ticket_price': row[5],
                'channel': row[6],
                'ticket_redeemed_at': row[7],
                'ticket_data': row[8]
            }
        return None

    def _save_ticket_to_db(self, conn: sqlite3.Connection, ticket: Dict, is_new: bool):
        cursor = conn.cursor()
        now = datetime.now().isoformat()

        ticket_id = ticket.get('ticket_id')
        event_id = str(ticket.get('event_id', 'unknown'))
        event_name = ticket.get('event_name', 'Unknown Event')
        ticket_title = self._normalize_ticket_title(ticket)
        ticket_status = ticket.get('ticket_status', 'unknown')
        ticket_price = ticket.get('ticket_price', 0)
        channel = ticket.get('channel', 'unknown')
        ticket_redeemed_at = ticket.get('ticket_redeemed_at')

        filtered_ticket = self._filter_personal_data(ticket)
        ticket_data = json.dumps(filtered_ticket)

        if is_new:
            cursor.execute('''
                INSERT INTO tickets (
                    ticket_id, event_id, event_name, ticket_title, ticket_status,
                    ticket_price, channel, ticket_redeemed_at, ticket_data,
                    first_seen_at, last_updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (ticket_id, event_id, event_name, ticket_title, ticket_status,
                  ticket_price, channel, ticket_redeemed_at, ticket_data, now, now))
        else:
            cursor.execute('''
                UPDATE tickets SET
                    event_name = ?, ticket_title = ?, ticket_status = ?,
                    ticket_price = ?, channel = ?, ticket_redeemed_at = ?,
                    ticket_data = ?, last_updated_at = ?
                WHERE ticket_id = ?
            ''', (event_name, ticket_title, ticket_status, ticket_price,
                  channel, ticket_redeemed_at, ticket_data, now, ticket_id))

    def _record_status_change(self, conn: sqlite3.Connection, ticket_id: str,
                             old_status: str, new_status: str):
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO ticket_status_changes (ticket_id, old_status, new_status, changed_at)
            VALUES (?, ?, ?, ?)
        ''', (ticket_id, old_status, new_status, datetime.now().isoformat()))

    def process_new_tickets(self, tickets: List[Dict]):
        new_tickets_count = 0
        updated_tickets_count = 0
        refunds_detected = 0

        conn = sqlite3.connect(self.DB_FILE)
        try:
            for ticket in tickets:
                ticket_id = ticket.get('ticket_id')
                if not ticket_id:
                    continue

                event_id = str(ticket.get('event_id', 'unknown'))
                event_name = ticket.get('event_name', 'Unknown Event')
                ticket_title = self._normalize_ticket_title(ticket)
                ticket_status = ticket.get('ticket_status', 'unknown')
                channel = ticket.get('channel', 'unknown')
                ticket_price = ticket.get('ticket_price', 0)
                redeemed = ticket.get('ticket_redeemed_at') is not None

                existing_ticket = self._get_ticket_from_db(conn, ticket_id)

                if existing_ticket is None:
                    new_tickets_count += 1
                    self._save_ticket_to_db(conn, ticket, is_new=True)

                    if ticket_status == 'valid':
                        tickets_sold_total.labels(
                            event_id=event_id,
                            event_name=event_name,
                            ticket_title=ticket_title
                        ).inc()

                        tickets_revenue_total.labels(
                            event_id=event_id,
                            event_name=event_name,
                            ticket_title=ticket_title
                        ).inc(ticket_price)

                        tickets_by_channel_total.labels(
                            event_id=event_id,
                            event_name=event_name,
                            channel=channel
                        ).inc()

                    elif ticket_status in ['refunded', 'canceled']:
                        tickets_refunded_total.labels(
                            event_id=event_id,
                            event_name=event_name,
                            ticket_title=ticket_title
                        ).inc()

                    if redeemed:
                        tickets_scanned_total.labels(
                            event_id=event_id,
                            event_name=event_name
                        ).inc()

                else:
                    old_status = existing_ticket['ticket_status']

                    if old_status != ticket_status:
                        updated_tickets_count += 1
                        self._save_ticket_to_db(conn, ticket, is_new=False)
                        self._record_status_change(conn, ticket_id, old_status, ticket_status)

                        logger.info(f"Status change detected for ticket {ticket_id}: {old_status} → {ticket_status}")

                        if old_status == 'valid' and ticket_status in ['refunded', 'canceled']:
                            refunds_detected += 1
                            tickets_refunded_total.labels(
                                event_id=event_id,
                                event_name=event_name,
                                ticket_title=ticket_title
                            ).inc()

                    old_redeemed = existing_ticket.get('ticket_redeemed_at') is not None
                    if redeemed and not old_redeemed:
                        tickets_scanned_total.labels(
                            event_id=event_id,
                            event_name=event_name
                        ).inc()
                        self._save_ticket_to_db(conn, ticket, is_new=False)

            conn.commit()
            logger.info(f"{new_tickets_count} new ticket(s), {updated_tickets_count} updated, {refunds_detected} refund(s) detected")

        except Exception as e:
            logger.error(f"Error processing tickets: {e}", exc_info=True)
            conn.rollback()
        finally:
            conn.close()

    def update_event_metrics(self, events: List[Dict]):
        logger.info("Updating event metrics...")

        events_total._metrics.clear()
        event_tickets_left._metrics.clear()

        active_events = 0
        past_events = 0
        cancelled_events = 0

        for event in events:
            event_id = str(event.get('id', 'unknown'))
            event_name = event.get('name', 'Unknown Event')
            tickets_left = event.get('leftTicketsCount', 0)
            cancelled_at = event.get('cancelledAt')
            start_time = event.get('startTime')

            if cancelled_at:
                cancelled_events += 1
            elif start_time:
                event_date = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
                if event_date < datetime.now(event_date.tzinfo):
                    past_events += 1
                else:
                    active_events += 1

            event_tickets_left.labels(
                event_id=event_id,
                event_name=event_name
            ).set(tickets_left)

        events_total.labels(status='active').set(active_events)
        events_total.labels(status='past').set(past_events)
        events_total.labels(status='cancelled').set(cancelled_events)

    def collect_metrics(self):
        logger.info("Starting metrics collection...")
        start_time = time.time()

        try:
            should_fetch_events = self._should_fetch_events()
            if should_fetch_events:
                events = self.fetch_events()
                self.update_event_metrics(events)
                self._mark_events_fetched()
            else:
                logger.info(f"Skipping events fetch (next fetch in {EVENTS_FETCH_INTERVAL/3600:.1f} hours)")

            do_full_scan = self._should_do_full_scan()
            do_recent_scan = self._should_do_recent_scan()

            # Priority: full scan > recent scan > incremental
            if do_full_scan:
                all_tickets = self.fetch_all_tickets(full_scan=True)
                self.process_new_tickets(all_tickets)
                self._mark_full_scan_done()
                logger.info(f"Full scan completed, next full scan in {FULL_SCAN_INTERVAL/3600:.1f} hours")
            elif do_recent_scan:
                recent_tickets = self.fetch_all_tickets(recent_only=True)
                self.process_new_tickets(recent_tickets)
                self._mark_recent_scan_done()
                logger.info(f"Recent scan completed, next recent scan in {RECENT_SCAN_INTERVAL/3600:.1f} hours")
            else:
                all_tickets = self.fetch_all_tickets(full_scan=False)
                self.process_new_tickets(all_tickets)

            last_scrape_timestamp.set(time.time())

            elapsed = time.time() - start_time
            logger.info(f"Metrics collection completed in {elapsed:.2f}s")

        except Exception as e:
            logger.error(f"Error during metrics collection: {e}", exc_info=True)

    def run(self):
        logger.info(f"Starting Shotgun exporter on port {EXPORTER_PORT}")
        logger.info(f"Scrape interval: {SCRAPE_INTERVAL} seconds")

        start_http_server(EXPORTER_PORT)

        # First run: always fetch events
        logger.info("Initial metrics collection (forcing events fetch)...")
        try:
            events = self.fetch_events()
            self.update_event_metrics(events)
            self._mark_events_fetched()
        except Exception as e:
            logger.error(f"Error during initial events fetch: {e}", exc_info=True)

        while True:
            try:
                self.collect_metrics()
            except Exception as e:
                logger.error(f"Error in main loop: {e}", exc_info=True)

            logger.info(f"Waiting {SCRAPE_INTERVAL} seconds before next collection...")
            time.sleep(SCRAPE_INTERVAL)


def main():
    try:
        exporter = ShotgunExporter()
        exporter.run()
    except KeyboardInterrupt:
        logger.info("Stopping exporter...")
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        exit(1)


if __name__ == '__main__':
    main()
