#!/usr/bin/env python3

import os
import time
import logging
import json
import sqlite3
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from pathlib import Path
from threading import Thread, Lock
import requests
from prometheus_client import start_http_server, Gauge, Counter
from dotenv import load_dotenv
from flask import Flask, jsonify
import sentry_sdk
from sentry_sdk.integrations.logging import LoggingIntegration

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

load_dotenv()

# Initialize Sentry if DSN is provided
SENTRY_DSN = os.getenv('SENTRY_DSN')
if SENTRY_DSN:
    sentry_sdk.init(
        dsn=SENTRY_DSN,
        environment=os.getenv('SENTRY_ENVIRONMENT', 'production'),
        traces_sample_rate=float(os.getenv('SENTRY_TRACES_SAMPLE_RATE', '0.1')),
        profiles_sample_rate=float(os.getenv('SENTRY_PROFILES_SAMPLE_RATE', '0.1')),
        integrations=[
            LoggingIntegration(
                level=logging.INFO,
                event_level=logging.ERROR
            ),
        ],
    )
    logger.info("Sentry monitoring enabled")
else:
    logger.info("Sentry monitoring disabled (no SENTRY_DSN provided)")

SHOTGUN_TOKEN = os.getenv('SHOTGUN_TOKEN')
SHOTGUN_ORGANIZER_ID = os.getenv('SHOTGUN_ORGANIZER_ID')
EXPORTER_PORT = int(os.getenv('EXPORTER_PORT', '9090'))
API_PORT = int(os.getenv('API_PORT', '9091'))
SCRAPE_INTERVAL = int(os.getenv('SCRAPE_INTERVAL', '60'))
INCLUDE_COHOSTED_EVENTS = os.getenv('INCLUDE_COHOSTED_EVENTS', 'false').lower() == 'true'
FULL_SCAN_INTERVAL = int(os.getenv('FULL_SCAN_INTERVAL', '86400'))
EVENTS_FETCH_INTERVAL = int(os.getenv('EVENTS_FETCH_INTERVAL', '3600'))

# Rate limiting: 200 calls/min = 1 call per 300ms minimum
API_RATE_LIMIT_DELAY = 0.3  # seconds between API calls

# API URLs
TICKETS_URL = "https://api.shotgun.live/tickets"
EVENTS_URL = f"https://smartboard-api.shotgun.live/api/shotgun/organizers/{SHOTGUN_ORGANIZER_ID}/events"

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

tickets_by_payment_method_total = Counter(
    'shotgun_tickets_by_payment_method_total',
    'Number of tickets by payment method',
    ['event_id', 'event_name', 'payment_method']
)

tickets_by_utm_source_total = Counter(
    'shotgun_tickets_by_utm_source_total',
    'Number of tickets by UTM source',
    ['event_id', 'event_name', 'utm_source']
)

tickets_by_utm_medium_total = Counter(
    'shotgun_tickets_by_utm_medium_total',
    'Number of tickets by UTM medium',
    ['event_id', 'event_name', 'utm_medium']
)

tickets_by_visibility_total = Counter(
    'shotgun_tickets_by_visibility_total',
    'Number of tickets by visibility/sale type',
    ['event_id', 'event_name', 'visibility']
)

tickets_fees_total = Counter(
    'shotgun_tickets_fees_euros_total',
    'Total fees collected in euros',
    ['event_id', 'event_name', 'fee_type']
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

api_requests_total = Counter(
    'shotgun_api_requests_total',
    'Total number of requests to Shotgun API',
    ['endpoint', 'status']
)

last_scrape_timestamp = Gauge(
    'shotgun_last_scrape_timestamp',
    'Timestamp of last successful scrape'
)

# Flask API for manual triggers
app = Flask(__name__)
exporter_instance = None


class ShotgunExporter:
    DB_FILE = Path('/data/shotgun_tickets.db')

    def __init__(self):
        if not SHOTGUN_TOKEN:
            raise ValueError("SHOTGUN_TOKEN must be defined in .env file")
        if not SHOTGUN_ORGANIZER_ID:
            raise ValueError("SHOTGUN_ORGANIZER_ID must be defined in .env file")

        self.session = requests.Session()
        self.session.headers['Authorization'] = f'Bearer {SHOTGUN_TOKEN}'
        self._scan_lock = Lock()  # Prevent concurrent scans
        self._last_api_call = 0  # For rate limiting
        self._events_cache = {}  # In-memory cache for event_id -> event_name

        self._init_database()

    def _init_database(self):
        try:
            conn = sqlite3.connect(self.DB_FILE)
            cursor = conn.cursor()

            # New schema for the new API
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

            # Events cache table for event_id -> event_name mapping
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS events_cache (
                    event_id INTEGER PRIMARY KEY,
                    event_name TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
            ''')

            cursor.execute('CREATE INDEX IF NOT EXISTS idx_event_id ON tickets(event_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_status ON tickets(ticket_status)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_ticket_updated_at ON tickets(ticket_updated_at)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_status_changes_ticket ON ticket_status_changes(ticket_id)')

            conn.commit()

            cursor.execute('SELECT COUNT(*) FROM tickets')
            count = cursor.fetchone()[0]
            logger.info(f"Database initialized: {count} tickets in database")

            # Load events cache into memory
            self._load_events_cache(conn)

            if count > 0:
                self._restore_counters_from_db(conn)

            conn.close()
        except Exception as e:
            logger.error(f"Error initializing database: {e}")
            raise

    def _load_events_cache(self, conn: sqlite3.Connection):
        """Load events cache from database into memory"""
        cursor = conn.cursor()
        cursor.execute('SELECT event_id, event_name FROM events_cache')
        for row in cursor.fetchall():
            self._events_cache[row[0]] = row[1]
        logger.info(f"Loaded {len(self._events_cache)} events into cache")

    def _get_event_name(self, event_id: int) -> str:
        """Get event name from cache, returns 'Unknown Event' if not found"""
        return self._events_cache.get(event_id, 'Unknown Event')

    def _update_events_cache(self, events: List[Dict]):
        """Update both in-memory and database events cache"""
        conn = sqlite3.connect(self.DB_FILE)
        cursor = conn.cursor()
        now = datetime.now().isoformat()

        for event in events:
            event_id = event.get('id')
            event_name = event.get('name', 'Unknown Event')
            if event_id:
                self._events_cache[event_id] = event_name
                cursor.execute('''
                    INSERT OR REPLACE INTO events_cache (event_id, event_name, updated_at)
                    VALUES (?, ?, ?)
                ''', (event_id, event_name, now))

        conn.commit()
        conn.close()
        logger.info(f"Updated events cache: {len(events)} events")

    def _restore_counters_from_db(self, conn: sqlite3.Connection):
        cursor = conn.cursor()

        # Tickets sold and revenue (deal_price is in cents, convert to euros)
        cursor.execute('''
            SELECT event_id, deal_title, COUNT(*), SUM(deal_price)
            FROM tickets
            WHERE ticket_status = 'valid'
            GROUP BY event_id, deal_title
        ''')
        for row in cursor.fetchall():
            event_id, deal_title, count, total_revenue_cents = row
            event_name = self._get_event_name(event_id)
            ticket_title = deal_title or 'Unknown Ticket'
            tickets_sold_total.labels(
                event_id=str(event_id),
                event_name=event_name,
                ticket_title=ticket_title
            ).inc(count)
            # Convert cents to euros
            total_revenue_euros = (total_revenue_cents or 0) / 100.0
            tickets_revenue_total.labels(
                event_id=str(event_id),
                event_name=event_name,
                ticket_title=ticket_title
            ).inc(total_revenue_euros)

        # By channel
        cursor.execute('''
            SELECT event_id, deal_channel, COUNT(*)
            FROM tickets
            WHERE ticket_status = 'valid'
            GROUP BY event_id, deal_channel
        ''')
        for row in cursor.fetchall():
            event_id, channel, count = row
            event_name = self._get_event_name(event_id)
            tickets_by_channel_total.labels(
                event_id=str(event_id),
                event_name=event_name,
                channel=channel or 'unknown'
            ).inc(count)

        # Refunded tickets
        cursor.execute('''
            SELECT event_id, deal_title, COUNT(*)
            FROM tickets
            WHERE ticket_status IN ('refunded', 'canceled')
            GROUP BY event_id, deal_title
        ''')
        for row in cursor.fetchall():
            event_id, deal_title, count = row
            event_name = self._get_event_name(event_id)
            ticket_title = deal_title or 'Unknown Ticket'
            tickets_refunded_total.labels(
                event_id=str(event_id),
                event_name=event_name,
                ticket_title=ticket_title
            ).inc(count)

        # Scanned tickets
        cursor.execute('''
            SELECT event_id, COUNT(*)
            FROM tickets
            WHERE ticket_scanned_at IS NOT NULL
            GROUP BY event_id
        ''')
        for row in cursor.fetchall():
            event_id, count = row
            event_name = self._get_event_name(event_id)
            tickets_scanned_total.labels(
                event_id=str(event_id),
                event_name=event_name
            ).inc(count)

        # By payment method
        cursor.execute('''
            SELECT event_id, payment_method, COUNT(*)
            FROM tickets
            WHERE ticket_status = 'valid'
            GROUP BY event_id, payment_method
        ''')
        for row in cursor.fetchall():
            event_id, payment_method, count = row
            event_name = self._get_event_name(event_id)
            tickets_by_payment_method_total.labels(
                event_id=str(event_id),
                event_name=event_name,
                payment_method=payment_method or 'unknown'
            ).inc(count)

        # By UTM source
        cursor.execute('''
            SELECT event_id, utm_source, COUNT(*)
            FROM tickets
            WHERE ticket_status = 'valid'
            GROUP BY event_id, utm_source
        ''')
        for row in cursor.fetchall():
            event_id, utm_source, count = row
            event_name = self._get_event_name(event_id)
            tickets_by_utm_source_total.labels(
                event_id=str(event_id),
                event_name=event_name,
                utm_source=utm_source or 'unknown'
            ).inc(count)

        # By UTM medium
        cursor.execute('''
            SELECT event_id, utm_medium, COUNT(*)
            FROM tickets
            WHERE ticket_status = 'valid'
            GROUP BY event_id, utm_medium
        ''')
        for row in cursor.fetchall():
            event_id, utm_medium, count = row
            event_name = self._get_event_name(event_id)
            tickets_by_utm_medium_total.labels(
                event_id=str(event_id),
                event_name=event_name,
                utm_medium=utm_medium or 'unknown'
            ).inc(count)

        # By visibility (need to parse JSON array)
        cursor.execute('''
            SELECT event_id, deal_visibilities, COUNT(*)
            FROM tickets
            WHERE ticket_status = 'valid' AND deal_visibilities IS NOT NULL
            GROUP BY event_id, deal_visibilities
        ''')
        for row in cursor.fetchall():
            event_id, visibilities_json, count = row
            event_name = self._get_event_name(event_id)
            try:
                visibilities = json.loads(visibilities_json) if visibilities_json else []
                for visibility in visibilities:
                    tickets_by_visibility_total.labels(
                        event_id=str(event_id),
                        event_name=event_name,
                        visibility=visibility
                    ).inc(count)
            except json.JSONDecodeError:
                pass

        # Fees (convert cents to euros)
        cursor.execute('''
            SELECT event_id, SUM(deal_service_fee), SUM(deal_user_service_fee)
            FROM tickets
            WHERE ticket_status = 'valid'
            GROUP BY event_id
        ''')
        for row in cursor.fetchall():
            event_id, service_fee_cents, user_service_fee_cents = row
            event_name = self._get_event_name(event_id)
            if service_fee_cents:
                tickets_fees_total.labels(
                    event_id=str(event_id),
                    event_name=event_name,
                    fee_type='service_fee'
                ).inc(service_fee_cents / 100.0)
            if user_service_fee_cents:
                tickets_fees_total.labels(
                    event_id=str(event_id),
                    event_name=event_name,
                    fee_type='user_service_fee'
                ).inc(user_service_fee_cents / 100.0)

        logger.info("Counters restored from database")

    def _rate_limit(self):
        """Enforce rate limiting: 200 calls/min = minimum 300ms between calls"""
        now = time.time()
        elapsed = now - self._last_api_call
        if elapsed < API_RATE_LIMIT_DELAY:
            sleep_time = API_RATE_LIMIT_DELAY - elapsed
            time.sleep(sleep_time)
        self._last_api_call = time.time()

    def _make_request(self, url: str, params: Optional[Dict] = None, max_retries: int = 3, use_token: bool = True) -> Optional[Dict]:
        """Make HTTP request with retry logic and rate limiting.
        
        Args:
            url: The URL to request
            params: Query parameters
            max_retries: Number of retry attempts
            use_token: If True, use Bearer token auth (for new API). If False, use key param (for legacy events API).
        """
        for attempt in range(max_retries):
            try:
                # Rate limiting
                self._rate_limit()

                # Build params based on API type
                if use_token:
                    # New tickets API uses Bearer token (already in session headers)
                    full_params = params.copy() if params else {}
                else:
                    # Legacy events API uses key param
                    full_params = {'key': SHOTGUN_TOKEN}
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
                logger.warning(f"Timeout during request (attempt {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    wait_time = 2 ** attempt  # Exponential backoff: 1s, 2s, 4s
                    logger.info(f"Retrying in {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    logger.error(f"Request failed after {max_retries} attempts (timeout)")
                    api_requests_total.labels(endpoint=url.split('/')[-1], status='timeout').inc()
                    return None

            except requests.exceptions.RequestException as e:
                # Retry on 5xx errors (server errors), but not on 4xx (client errors)
                if hasattr(e, 'response') and e.response is not None:
                    status_code = e.response.status_code
                    if 500 <= status_code < 600:
                        logger.warning(f"Server error {status_code} (attempt {attempt + 1}/{max_retries}): {e}")
                        if attempt < max_retries - 1:
                            wait_time = 2 ** attempt  # Exponential backoff
                            logger.info(f"Retrying in {wait_time}s...")
                            time.sleep(wait_time)
                            continue

                logger.error(f"Error during request: {e}")
                api_requests_total.labels(endpoint=url.split('/')[-1], status='error').inc()
                return None

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

    def _get_last_ticket_after(self) -> Optional[str]:
        """Get the 'after' parameter for incremental scans (last ticket_updated_at_ticket_id)"""
        try:
            conn = sqlite3.connect(self.DB_FILE)
            cursor = conn.cursor()
            cursor.execute("SELECT value FROM exporter_state WHERE key = 'last_ticket_after'")
            row = cursor.fetchone()
            conn.close()
            return row[0] if row else None
        except Exception as e:
            logger.warning(f"Error reading last ticket after: {e}")
            return None

    def _save_last_ticket_after(self, after_value: str):
        """Save the 'after' parameter for next incremental scan"""
        try:
            conn = sqlite3.connect(self.DB_FILE)
            cursor = conn.cursor()
            now = datetime.now().isoformat()
            cursor.execute('''
                INSERT OR REPLACE INTO exporter_state (key, value, updated_at)
                VALUES ('last_ticket_after', ?, ?)
            ''', (after_value, now))
            conn.commit()
            conn.close()
        except Exception as e:
            logger.error(f"Error saving last ticket after: {e}")

    def _extract_after_from_url(self, url: str) -> Optional[str]:
        """Extract 'after' parameter from pagination URL"""
        from urllib.parse import urlparse, parse_qs, unquote
        try:
            parsed = urlparse(url)
            query_params = parse_qs(parsed.query)
            after_values = query_params.get('after', [])
            if after_values:
                return unquote(after_values[0])
        except Exception as e:
            logger.warning(f"Could not parse after from URL: {e}")
        return None

    def fetch_all_tickets(self, full_scan: bool = False) -> List[Dict]:
        """Fetch tickets from the new /tickets API.
        
        Args:
            full_scan: If True, fetch all tickets from the beginning.
                      If False, use incremental mode with 'after' parameter.
        """
        all_tickets = []

        params = {
            'organizer_id': SHOTGUN_ORGANIZER_ID,
        }

        if INCLUDE_COHOSTED_EVENTS:
            params['include_cohosted_events'] = 'true'

        # For incremental mode, use the last 'after' value
        if not full_scan:
            last_after = self._get_last_ticket_after()
            if last_after:
                params['after'] = last_after
                logger.info(f"Incremental scan starting from: {last_after[:50]}...")

        scan_mode = "full scan" if full_scan else "incremental"
        if INCLUDE_COHOSTED_EVENTS:
            logger.info(f"Fetching tickets ({scan_mode}, including co-hosted events)...")
        else:
            logger.info(f"Fetching tickets ({scan_mode})...")

        page_count = 0
        last_after_value = None

        while True:
            try:
                data = self._make_request(TICKETS_URL, params, use_token=True)
                if not data:
                    logger.warning(f"No data received at page {page_count + 1}, stopping pagination")
                    break

                tickets = data.get('data', [])
                if not tickets:
                    logger.info(f"No tickets at page {page_count + 1}, end of pagination")
                    break

                all_tickets.extend(tickets)
                page_count += 1

                logger.info(f"Page {page_count}: {len(tickets)} tickets fetched (total: {len(all_tickets)})")

                # Track the last ticket for next incremental scan
                if tickets:
                    last_ticket = tickets[-1]
                    ticket_updated_at = last_ticket.get('ticket_updated_at', '')
                    ticket_id = last_ticket.get('ticket_id', '')
                    if ticket_updated_at and ticket_id:
                        last_after_value = f"{ticket_updated_at}_{ticket_id}"

                # Check for next page
                pagination_info = data.get('pagination', {})
                next_url = pagination_info.get('next')
                if not next_url:
                    logger.info("No next page, end of pagination")
                    break

                # Extract 'after' from next URL
                next_after = self._extract_after_from_url(next_url)
                if next_after:
                    params['after'] = next_after
                    logger.debug(f"After for next page: {next_after[:50]}...")
                else:
                    logger.warning("Could not extract 'after' from next URL, stopping")
                    break

            except Exception as e:
                logger.error(f"Error fetching page {page_count + 1}: {e}")
                break

        # Save last after value for next incremental scan
        if last_after_value:
            self._save_last_ticket_after(last_after_value)
            logger.debug(f"Saved last_ticket_after: {last_after_value[:50]}...")

        logger.info(f"Total: {len(all_tickets)} tickets fetched in {page_count} page(s)")
        return all_tickets

    def fetch_events(self) -> List[Dict]:
        """Fetch events from the legacy smartboard API (uses key param auth)"""
        logger.info("Fetching events...")

        future_events_data = self._make_request(EVENTS_URL, use_token=False)
        future_events = future_events_data.get('data', []) if future_events_data else []

        past_events_data = self._make_request(EVENTS_URL, {'past_events': 'true', 'limit': 100}, use_token=False)
        past_events = past_events_data.get('data', []) if past_events_data else []

        all_events = future_events + past_events
        logger.info(f"Total: {len(all_events)} events fetched")

        # Update events cache
        self._update_events_cache(all_events)

        return all_events

    def _normalize_ticket_title(self, ticket: Dict) -> str:
        """Normalize ticket title using deal_title or deal_sub_category fallback"""
        import re
        ticket_title = ticket.get('deal_title', 'Unknown Ticket')

        # If title starts with 3+ digits, use sub_category as fallback
        if re.match(r'^\d{3,}', str(ticket_title)):
            deal_sub_category = ticket.get('deal_sub_category')
            if deal_sub_category:
                return deal_sub_category

        return ticket_title if ticket_title else 'Unknown Ticket'

    def _filter_personal_data(self, ticket: Dict) -> Dict:
        """Remove personal data fields from ticket before storing.
        Note: The new API doesn't include buyer info, but we keep this for safety."""
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

    def _get_ticket_from_db(self, conn: sqlite3.Connection, ticket_id: int) -> Optional[Dict]:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT ticket_id, event_id, ticket_status, ticket_scanned_at,
                   deal_title, deal_channel, deal_price, ticket_data
            FROM tickets WHERE ticket_id = ?
        ''', (ticket_id,))
        row = cursor.fetchone()
        if row:
            return {
                'ticket_id': row[0],
                'event_id': row[1],
                'ticket_status': row[2],
                'ticket_scanned_at': row[3],
                'deal_title': row[4],
                'deal_channel': row[5],
                'deal_price': row[6],
                'ticket_data': row[7]
            }
        return None

    def _save_ticket_to_db(self, conn: sqlite3.Connection, ticket: Dict, is_new: bool):
        """Save ticket to database using the new API schema"""
        cursor = conn.cursor()
        now = datetime.now().isoformat()

        ticket_id = ticket.get('ticket_id')
        event_id = ticket.get('event_id')
        ticket_status = ticket.get('ticket_status', 'unknown')
        ticket_scanned_at = ticket.get('ticket_scanned_at')
        ticket_updated_at = ticket.get('ticket_updated_at')
        ticket_canceled_at = ticket.get('ticket_canceled_at')
        deal_id = ticket.get('deal_id')
        deal_title = ticket.get('deal_title')
        deal_sub_category = ticket.get('deal_sub_category')
        deal_channel = ticket.get('deal_channel')
        deal_visibilities = json.dumps(ticket.get('deal_visibilities', []))
        deal_price = ticket.get('deal_price', 0)  # in cents
        deal_service_fee = ticket.get('deal_service_fee', 0)
        deal_user_service_fee = ticket.get('deal_user_service_fee', 0)
        currency = ticket.get('currency')
        payment_method = ticket.get('payment_method')
        utm_source = ticket.get('utm_source')
        utm_medium = ticket.get('utm_medium')
        order_id = ticket.get('order_id')
        ordered_at = ticket.get('ordered_at')

        filtered_ticket = self._filter_personal_data(ticket)
        ticket_data = json.dumps(filtered_ticket)

        if is_new:
            cursor.execute('''
                INSERT INTO tickets (
                    ticket_id, event_id, ticket_status, ticket_scanned_at, ticket_updated_at,
                    ticket_canceled_at, deal_id, deal_title, deal_sub_category, deal_channel,
                    deal_visibilities, deal_price, deal_service_fee, deal_user_service_fee,
                    currency, payment_method, utm_source, utm_medium, order_id, ordered_at,
                    ticket_data, first_seen_at, last_updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (ticket_id, event_id, ticket_status, ticket_scanned_at, ticket_updated_at,
                  ticket_canceled_at, deal_id, deal_title, deal_sub_category, deal_channel,
                  deal_visibilities, deal_price, deal_service_fee, deal_user_service_fee,
                  currency, payment_method, utm_source, utm_medium, order_id, ordered_at,
                  ticket_data, now, now))
        else:
            cursor.execute('''
                UPDATE tickets SET
                    ticket_status = ?, ticket_scanned_at = ?, ticket_updated_at = ?,
                    ticket_canceled_at = ?, deal_title = ?, deal_sub_category = ?,
                    deal_channel = ?, deal_visibilities = ?, deal_price = ?,
                    deal_service_fee = ?, deal_user_service_fee = ?, payment_method = ?,
                    utm_source = ?, utm_medium = ?, ticket_data = ?, last_updated_at = ?
                WHERE ticket_id = ?
            ''', (ticket_status, ticket_scanned_at, ticket_updated_at, ticket_canceled_at,
                  deal_title, deal_sub_category, deal_channel, deal_visibilities, deal_price,
                  deal_service_fee, deal_user_service_fee, payment_method, utm_source,
                  utm_medium, ticket_data, now, ticket_id))

    def _record_status_change(self, conn: sqlite3.Connection, ticket_id: int,
                             old_status: str, new_status: str):
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO ticket_status_changes (ticket_id, old_status, new_status, changed_at)
            VALUES (?, ?, ?, ?)
        ''', (ticket_id, old_status, new_status, datetime.now().isoformat()))

    def process_new_tickets(self, tickets: List[Dict]):
        """Process tickets from the new API and update metrics"""
        new_tickets_count = 0
        updated_tickets_count = 0
        refunds_detected = 0

        conn = sqlite3.connect(self.DB_FILE)
        try:
            for ticket in tickets:
                ticket_id = ticket.get('ticket_id')
                if not ticket_id:
                    continue

                # Get event info
                event_id = ticket.get('event_id')
                event_name = self._get_event_name(event_id)
                
                # Get ticket info with new field names
                ticket_title = self._normalize_ticket_title(ticket)
                ticket_status = ticket.get('ticket_status', 'unknown')
                channel = ticket.get('deal_channel', 'unknown')
                deal_price_cents = ticket.get('deal_price', 0)
                deal_price_euros = deal_price_cents / 100.0  # Convert cents to euros
                scanned = ticket.get('ticket_scanned_at') is not None
                
                # New fields for additional metrics
                payment_method = ticket.get('payment_method', 'unknown')
                utm_source = ticket.get('utm_source', 'unknown')
                utm_medium = ticket.get('utm_medium', 'unknown')
                visibilities = ticket.get('deal_visibilities', [])
                service_fee_cents = ticket.get('deal_service_fee', 0)
                user_service_fee_cents = ticket.get('deal_user_service_fee', 0)

                existing_ticket = self._get_ticket_from_db(conn, ticket_id)

                if existing_ticket is None:
                    new_tickets_count += 1
                    self._save_ticket_to_db(conn, ticket, is_new=True)

                    if ticket_status == 'valid':
                        # Core metrics
                        tickets_sold_total.labels(
                            event_id=str(event_id),
                            event_name=event_name,
                            ticket_title=ticket_title
                        ).inc()

                        tickets_revenue_total.labels(
                            event_id=str(event_id),
                            event_name=event_name,
                            ticket_title=ticket_title
                        ).inc(deal_price_euros)

                        tickets_by_channel_total.labels(
                            event_id=str(event_id),
                            event_name=event_name,
                            channel=channel or 'unknown'
                        ).inc()

                        # New metrics
                        tickets_by_payment_method_total.labels(
                            event_id=str(event_id),
                            event_name=event_name,
                            payment_method=payment_method or 'unknown'
                        ).inc()

                        tickets_by_utm_source_total.labels(
                            event_id=str(event_id),
                            event_name=event_name,
                            utm_source=utm_source or 'unknown'
                        ).inc()

                        tickets_by_utm_medium_total.labels(
                            event_id=str(event_id),
                            event_name=event_name,
                            utm_medium=utm_medium or 'unknown'
                        ).inc()

                        # Visibility metrics (one entry per visibility type)
                        for visibility in visibilities:
                            tickets_by_visibility_total.labels(
                                event_id=str(event_id),
                                event_name=event_name,
                                visibility=visibility
                            ).inc()

                        # Fees metrics (in euros)
                        if service_fee_cents:
                            tickets_fees_total.labels(
                                event_id=str(event_id),
                                event_name=event_name,
                                fee_type='service_fee'
                            ).inc(service_fee_cents / 100.0)

                        if user_service_fee_cents:
                            tickets_fees_total.labels(
                                event_id=str(event_id),
                                event_name=event_name,
                                fee_type='user_service_fee'
                            ).inc(user_service_fee_cents / 100.0)

                    elif ticket_status in ['refunded', 'canceled', 'resold']:
                        tickets_refunded_total.labels(
                            event_id=str(event_id),
                            event_name=event_name,
                            ticket_title=ticket_title
                        ).inc()

                    if scanned:
                        tickets_scanned_total.labels(
                            event_id=str(event_id),
                            event_name=event_name
                        ).inc()

                else:
                    old_status = existing_ticket['ticket_status']

                    if old_status != ticket_status:
                        updated_tickets_count += 1
                        self._save_ticket_to_db(conn, ticket, is_new=False)
                        self._record_status_change(conn, ticket_id, old_status, ticket_status)

                        logger.info(f"Status change detected for ticket {ticket_id}: {old_status} -> {ticket_status}")

                        if old_status == 'valid' and ticket_status in ['refunded', 'canceled', 'resold']:
                            refunds_detected += 1
                            tickets_refunded_total.labels(
                                event_id=str(event_id),
                                event_name=event_name,
                                ticket_title=ticket_title
                            ).inc()

                    old_scanned = existing_ticket.get('ticket_scanned_at') is not None
                    if scanned and not old_scanned:
                        tickets_scanned_total.labels(
                            event_id=str(event_id),
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
        # Try to acquire lock, skip if another scan is running
        if not self._scan_lock.acquire(blocking=False):
            logger.warning("Skipping metrics collection: another scan is already running")
            return

        try:
            logger.info("Starting metrics collection...")
            start_time = time.time()
            # Events fetch monitoring
            should_fetch_events = self._should_fetch_events()
            if should_fetch_events:
                if SENTRY_DSN:
                    monitor_slug = 'shotgun-events-fetch'
                    check_in_id = sentry_sdk.crons.capture_checkin(
                        monitor_slug=monitor_slug,
                        status=sentry_sdk.crons.MonitorStatus.IN_PROGRESS,
                    )
                try:
                    events = self.fetch_events()
                    self.update_event_metrics(events)
                    self._mark_events_fetched()
                    if SENTRY_DSN:
                        sentry_sdk.crons.capture_checkin(
                            check_in_id=check_in_id,
                            monitor_slug=monitor_slug,
                            status=sentry_sdk.crons.MonitorStatus.OK,
                        )
                except Exception as e:
                    if SENTRY_DSN:
                        sentry_sdk.crons.capture_checkin(
                            check_in_id=check_in_id,
                            monitor_slug=monitor_slug,
                            status=sentry_sdk.crons.MonitorStatus.ERROR,
                        )
                    raise
            else:
                logger.info(f"Skipping events fetch (next fetch in {EVENTS_FETCH_INTERVAL/3600:.1f} hours)")

            do_full_scan = self._should_do_full_scan()

            # Full scan or incremental (using 'after' parameter)
            if do_full_scan:
                if SENTRY_DSN:
                    monitor_slug = 'shotgun-full-scan'
                    check_in_id = sentry_sdk.crons.capture_checkin(
                        monitor_slug=monitor_slug,
                        status=sentry_sdk.crons.MonitorStatus.IN_PROGRESS,
                    )
                try:
                    all_tickets = self.fetch_all_tickets(full_scan=True)
                    self.process_new_tickets(all_tickets)
                    self._mark_full_scan_done()
                    logger.info(f"Full scan completed, next full scan in {FULL_SCAN_INTERVAL/3600:.1f} hours")
                    if SENTRY_DSN:
                        sentry_sdk.crons.capture_checkin(
                            check_in_id=check_in_id,
                            monitor_slug=monitor_slug,
                            status=sentry_sdk.crons.MonitorStatus.OK,
                        )
                except Exception as e:
                    if SENTRY_DSN:
                        sentry_sdk.crons.capture_checkin(
                            check_in_id=check_in_id,
                            monitor_slug=monitor_slug,
                            status=sentry_sdk.crons.MonitorStatus.ERROR,
                        )
                    raise
            else:
                # Incremental scan using 'after' parameter
                all_tickets = self.fetch_all_tickets(full_scan=False)
                self.process_new_tickets(all_tickets)

            last_scrape_timestamp.set(time.time())

            elapsed = time.time() - start_time
            logger.info(f"Metrics collection completed in {elapsed:.2f}s")

        except Exception as e:
            logger.error(f"Error during metrics collection: {e}", exc_info=True)
        finally:
            self._scan_lock.release()

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

    def trigger_full_scan(self):
        """Manually trigger a full scan"""
        logger.info("Manual full scan triggered via API")

        # Acquire lock, wait if needed
        with self._scan_lock:
            logger.info("Lock acquired, starting full scan...")
            try:
                if SENTRY_DSN:
                    monitor_slug = 'shotgun-full-scan'
                    check_in_id = sentry_sdk.crons.capture_checkin(
                        monitor_slug=monitor_slug,
                        status=sentry_sdk.crons.MonitorStatus.IN_PROGRESS,
                    )

                all_tickets = self.fetch_all_tickets(full_scan=True)
                self.process_new_tickets(all_tickets)
                self._mark_full_scan_done()

                if SENTRY_DSN:
                    sentry_sdk.crons.capture_checkin(
                        check_in_id=check_in_id,
                        monitor_slug=monitor_slug,
                        status=sentry_sdk.crons.MonitorStatus.OK,
                    )

                logger.info("Manual full scan completed successfully")
                return True
            except Exception as e:
                logger.error(f"Error during manual full scan: {e}", exc_info=True)
                if SENTRY_DSN:
                    sentry_sdk.crons.capture_checkin(
                        check_in_id=check_in_id,
                        monitor_slug=monitor_slug,
                        status=sentry_sdk.crons.MonitorStatus.ERROR,
                    )
                return False

    def trigger_incremental_scan(self):
        """Manually trigger an incremental scan"""
        logger.info("Manual incremental scan triggered via API")

        # Acquire lock, wait if needed
        with self._scan_lock:
            logger.info("Lock acquired, starting incremental scan...")
            try:
                all_tickets = self.fetch_all_tickets(full_scan=False)
                self.process_new_tickets(all_tickets)
                logger.info("Manual incremental scan completed successfully")
                return True
            except Exception as e:
                logger.error(f"Error during manual incremental scan: {e}", exc_info=True)
                return False

    def trigger_events_fetch(self):
        """Manually trigger events fetch"""
        logger.info("Manual events fetch triggered via API")

        # Acquire lock, wait if needed
        with self._scan_lock:
            logger.info("Lock acquired, starting events fetch...")
            try:
                if SENTRY_DSN:
                    monitor_slug = 'shotgun-events-fetch'
                    check_in_id = sentry_sdk.crons.capture_checkin(
                        monitor_slug=monitor_slug,
                        status=sentry_sdk.crons.MonitorStatus.IN_PROGRESS,
                    )

                events = self.fetch_events()
                self.update_event_metrics(events)
                self._mark_events_fetched()

                if SENTRY_DSN:
                    sentry_sdk.crons.capture_checkin(
                        check_in_id=check_in_id,
                        monitor_slug=monitor_slug,
                        status=sentry_sdk.crons.MonitorStatus.OK,
                    )

                logger.info("Manual events fetch completed successfully")
                return True
            except Exception as e:
                logger.error(f"Error during manual events fetch: {e}", exc_info=True)
                if SENTRY_DSN:
                    sentry_sdk.crons.capture_checkin(
                        check_in_id=check_in_id,
                        monitor_slug=monitor_slug,
                        status=sentry_sdk.crons.MonitorStatus.ERROR,
                    )
                return False


# Flask API endpoints
@app.route('/trigger/full-scan', methods=['POST'])
def api_trigger_full_scan():
    """Endpoint to manually trigger a full scan"""
    if exporter_instance is None:
        return jsonify({'status': 'error', 'message': 'Exporter not initialized'}), 503

    # Run in background thread to avoid blocking the request
    thread = Thread(target=exporter_instance.trigger_full_scan)
    thread.daemon = True
    thread.start()

    return jsonify({
        'status': 'success',
        'message': 'Full scan triggered',
        'scan_type': 'full'
    })

@app.route('/trigger/incremental', methods=['POST'])
def api_trigger_incremental_scan():
    """Endpoint to manually trigger an incremental scan"""
    if exporter_instance is None:
        return jsonify({'status': 'error', 'message': 'Exporter not initialized'}), 503

    thread = Thread(target=exporter_instance.trigger_incremental_scan)
    thread.daemon = True
    thread.start()

    return jsonify({
        'status': 'success',
        'message': 'Incremental scan triggered',
        'scan_type': 'incremental'
    })

@app.route('/trigger/events', methods=['POST'])
def api_trigger_events_fetch():
    """Endpoint to manually trigger events fetch"""
    if exporter_instance is None:
        return jsonify({'status': 'error', 'message': 'Exporter not initialized'}), 503

    thread = Thread(target=exporter_instance.trigger_events_fetch)
    thread.daemon = True
    thread.start()

    return jsonify({
        'status': 'success',
        'message': 'Events fetch triggered',
        'scan_type': 'events'
    })

@app.route('/health', methods=['GET'])
def api_health():
    """Health check endpoint"""
    return jsonify({
        'status': 'healthy',
        'exporter_initialized': exporter_instance is not None
    })


def main():
    global exporter_instance

    try:
        exporter_instance = ShotgunExporter()

        # Start Flask API in background thread
        api_thread = Thread(target=lambda: app.run(host='0.0.0.0', port=API_PORT, debug=False))
        api_thread.daemon = True
        api_thread.start()
        logger.info(f"API server started on port {API_PORT}")

        exporter_instance.run()
    except KeyboardInterrupt:
        logger.info("Stopping exporter...")
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        exit(1)


if __name__ == '__main__':
    main()
