#!/usr/bin/env python3
"""
Re-import Shotgun event metrics with original timestamps.
Adapted for the new /tickets API schema.
"""

import os
import sys
import json
import sqlite3
import argparse
import requests
from datetime import datetime
from typing import List, Dict, Tuple, Optional
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

DB_FILE = Path('/data/shotgun_tickets.db')
VICTORIA_METRICS_URL = os.getenv('VICTORIA_METRICS_URL', 'http://victoria-metrics:8428')


def get_all_events(conn: sqlite3.Connection) -> List[Tuple[int, str, int]]:
    """Get all events with ticket counts from database"""
    cursor = conn.cursor()
    
    # First get event names from cache
    cursor.execute('SELECT event_id, event_name FROM events_cache')
    event_names = {row[0]: row[1] for row in cursor.fetchall()}
    
    # Then get ticket counts per event
    cursor.execute('''
        SELECT event_id, COUNT(*) as ticket_count
        FROM tickets
        GROUP BY event_id
        ORDER BY event_id
    ''')
    
    results = []
    for row in cursor.fetchall():
        event_id = row[0]
        event_name = event_names.get(event_id, 'Unknown Event')
        ticket_count = row[1]
        results.append((event_id, event_name, ticket_count))
    
    return results


def list_events(conn: sqlite3.Connection):
    """Display all available events"""
    events = get_all_events(conn)

    if not events:
        print("No events found in database")
        return

    print("\nAvailable events:")
    print("-" * 80)
    for event_id, event_name, count in events:
        print(f"  {event_id:10} | {event_name[:50]:50} | {count:5} tickets")
    print("-" * 80)
    print(f"Total: {len(events)} events")


def get_event_name(conn: sqlite3.Connection, event_id: int) -> str:
    """Get event name from cache"""
    cursor = conn.cursor()
    cursor.execute('SELECT event_name FROM events_cache WHERE event_id = ?', (event_id,))
    row = cursor.fetchone()
    return row[0] if row else 'Unknown Event'


def get_event_tickets(conn: sqlite3.Connection, event_id: int) -> List[Dict]:
    """Get all tickets for an event with their full data"""
    cursor = conn.cursor()
    cursor.execute('''
        SELECT ticket_id, event_id, ticket_status, ticket_scanned_at,
               deal_title, deal_sub_category, deal_channel, deal_price,
               deal_service_fee, deal_user_service_fee, deal_visibilities,
               payment_method, utm_source, utm_medium, ordered_at,
               ticket_canceled_at, ticket_data, first_seen_at
        FROM tickets
        WHERE event_id = ?
        ORDER BY first_seen_at
    ''', (event_id,))

    tickets = []
    for row in cursor.fetchall():
        ticket_data = json.loads(row[16]) if row[16] else {}
        visibilities = json.loads(row[10]) if row[10] else []
        
        tickets.append({
            'ticket_id': row[0],
            'event_id': row[1],
            'ticket_status': row[2],
            'ticket_scanned_at': row[3],
            'deal_title': row[4],
            'deal_sub_category': row[5],
            'deal_channel': row[6],
            'deal_price': row[7],  # in cents
            'deal_service_fee': row[8],
            'deal_user_service_fee': row[9],
            'deal_visibilities': visibilities,
            'payment_method': row[11],
            'utm_source': row[12],
            'utm_medium': row[13],
            'ordered_at': row[14],
            'ticket_canceled_at': row[15],
            'ticket_data': ticket_data,
            'first_seen_at': row[17]
        })

    return tickets


def delete_event_metrics(event_id: int, event_name: str, dry_run: bool = False) -> bool:
    """Delete all metrics for an event from VictoriaMetrics"""
    metrics_to_delete = [
        'shotgun_tickets_sold_total',
        'shotgun_tickets_revenue_euros_total',
        'shotgun_tickets_by_channel_total',
        'shotgun_tickets_refunded_total',
        'shotgun_tickets_scanned_total',
        'shotgun_tickets_by_payment_method_total',
        'shotgun_tickets_by_utm_source_total',
        'shotgun_tickets_by_utm_medium_total',
        'shotgun_tickets_by_visibility_total',
        'shotgun_tickets_fees_euros_total'
    ]

    print(f"\n{'[DRY RUN] ' if dry_run else ''}Deleting metrics for event {event_id} ({event_name})...")

    for metric in metrics_to_delete:
        match_filter = f'{metric}{{event_id="{event_id}"}}'

        if dry_run:
            print(f"  Would delete: {match_filter}")
        else:
            try:
                url = f"{VICTORIA_METRICS_URL}/api/v1/admin/tsdb/delete_series"
                params = {'match[]': match_filter}
                response = requests.post(url, params=params, timeout=30)
                response.raise_for_status()
                print(f"  Deleted: {match_filter}")
            except Exception as e:
                print(f"  Error deleting {metric}: {e}")
                return False

    return True


def format_prometheus_line(metric_name: str, labels: Dict[str, str], value: float, timestamp_ms: int) -> str:
    """Format a single Prometheus metric line with timestamp"""
    # Escape label values
    escaped_labels = {k: str(v).replace('\\', '\\\\').replace('"', '\\"').replace('\n', '\\n')
                     for k, v in labels.items()}

    labels_str = ','.join(f'{k}="{v}"' for k, v in escaped_labels.items())
    return f"{metric_name}{{{labels_str}}} {value} {timestamp_ms}"


def get_timestamp_ms(iso_string: str) -> Optional[int]:
    """Convert ISO timestamp string to milliseconds since epoch"""
    if not iso_string:
        return None

    try:
        # Handle both formats: with and without timezone
        if iso_string.endswith('Z'):
            dt = datetime.fromisoformat(iso_string.replace('Z', '+00:00'))
        else:
            dt = datetime.fromisoformat(iso_string)

        # Convert to UTC timestamp in milliseconds
        return int(dt.timestamp() * 1000)
    except Exception as e:
        print(f"  Warning: Could not parse timestamp '{iso_string}': {e}")
        return None


def normalize_ticket_title(ticket: Dict) -> str:
    """Normalize ticket title using deal_title or deal_sub_category fallback"""
    import re
    ticket_title = ticket.get('deal_title', 'Unknown Ticket')

    if re.match(r'^\d{3,}', str(ticket_title)):
        deal_sub_category = ticket.get('deal_sub_category')
        if deal_sub_category:
            return deal_sub_category

    return ticket_title if ticket_title else 'Unknown Ticket'


def reimport_event_data(conn: sqlite3.Connection, event_id: int, dry_run: bool = False) -> bool:
    """Re-import all metrics for an event with original timestamps"""
    tickets = get_event_tickets(conn, event_id)

    if not tickets:
        print(f"No tickets found for event {event_id}")
        return False

    event_name = get_event_name(conn, event_id)
    print(f"\n{'[DRY RUN] ' if dry_run else ''}Re-importing {len(tickets)} tickets for event {event_id} ({event_name})...")

    # Build Prometheus exposition format data with timestamps
    lines = []

    for ticket in tickets:
        ordered_at = ticket.get('ordered_at')

        if not ordered_at:
            print(f"  Warning: Ticket {ticket['ticket_id']} has no ordered_at timestamp, skipping")
            continue

        timestamp_ms = get_timestamp_ms(ordered_at)
        if not timestamp_ms:
            continue

        ticket_title = normalize_ticket_title(ticket)
        base_labels = {
            'event_id': str(event_id),
            'event_name': event_name,
            'ticket_title': ticket_title
        }

        # Sold tickets (only if valid)
        if ticket['ticket_status'] == 'valid':
            lines.append(format_prometheus_line(
                'shotgun_tickets_sold_total',
                base_labels,
                1,
                timestamp_ms
            ))

            # Revenue (convert cents to euros)
            deal_price_euros = (ticket.get('deal_price') or 0) / 100.0
            lines.append(format_prometheus_line(
                'shotgun_tickets_revenue_euros_total',
                base_labels,
                deal_price_euros,
                timestamp_ms
            ))

            # By channel
            channel_labels = {
                'event_id': str(event_id),
                'event_name': event_name,
                'channel': ticket.get('deal_channel') or 'unknown'
            }
            lines.append(format_prometheus_line(
                'shotgun_tickets_by_channel_total',
                channel_labels,
                1,
                timestamp_ms
            ))

            # By payment method
            payment_labels = {
                'event_id': str(event_id),
                'event_name': event_name,
                'payment_method': ticket.get('payment_method') or 'unknown'
            }
            lines.append(format_prometheus_line(
                'shotgun_tickets_by_payment_method_total',
                payment_labels,
                1,
                timestamp_ms
            ))

            # By UTM source
            utm_source_labels = {
                'event_id': str(event_id),
                'event_name': event_name,
                'utm_source': ticket.get('utm_source') or 'unknown'
            }
            lines.append(format_prometheus_line(
                'shotgun_tickets_by_utm_source_total',
                utm_source_labels,
                1,
                timestamp_ms
            ))

            # By UTM medium
            utm_medium_labels = {
                'event_id': str(event_id),
                'event_name': event_name,
                'utm_medium': ticket.get('utm_medium') or 'unknown'
            }
            lines.append(format_prometheus_line(
                'shotgun_tickets_by_utm_medium_total',
                utm_medium_labels,
                1,
                timestamp_ms
            ))

            # By visibility
            visibilities = ticket.get('deal_visibilities') or []
            for visibility in visibilities:
                visibility_labels = {
                    'event_id': str(event_id),
                    'event_name': event_name,
                    'visibility': visibility
                }
                lines.append(format_prometheus_line(
                    'shotgun_tickets_by_visibility_total',
                    visibility_labels,
                    1,
                    timestamp_ms
                ))

            # Fees (convert cents to euros)
            service_fee = ticket.get('deal_service_fee') or 0
            if service_fee:
                service_fee_labels = {
                    'event_id': str(event_id),
                    'event_name': event_name,
                    'fee_type': 'service_fee'
                }
                lines.append(format_prometheus_line(
                    'shotgun_tickets_fees_euros_total',
                    service_fee_labels,
                    service_fee / 100.0,
                    timestamp_ms
                ))

            user_service_fee = ticket.get('deal_user_service_fee') or 0
            if user_service_fee:
                user_fee_labels = {
                    'event_id': str(event_id),
                    'event_name': event_name,
                    'fee_type': 'user_service_fee'
                }
                lines.append(format_prometheus_line(
                    'shotgun_tickets_fees_euros_total',
                    user_fee_labels,
                    user_service_fee / 100.0,
                    timestamp_ms
                ))

        # Refunded tickets
        elif ticket['ticket_status'] in ['refunded', 'canceled', 'resold']:
            # Use ticket_canceled_at if available, otherwise ordered_at
            canceled_at = ticket.get('ticket_canceled_at') or ordered_at
            refund_timestamp_ms = get_timestamp_ms(canceled_at)

            if refund_timestamp_ms:
                lines.append(format_prometheus_line(
                    'shotgun_tickets_refunded_total',
                    base_labels,
                    1,
                    refund_timestamp_ms
                ))

        # Scanned tickets
        if ticket.get('ticket_scanned_at'):
            scan_timestamp_ms = get_timestamp_ms(ticket['ticket_scanned_at'])
            if scan_timestamp_ms:
                scan_labels = {
                    'event_id': str(event_id),
                    'event_name': event_name
                }
                lines.append(format_prometheus_line(
                    'shotgun_tickets_scanned_total',
                    scan_labels,
                    1,
                    scan_timestamp_ms
                ))

    if not lines:
        print("  No valid data to import")
        return False

    print(f"  Generated {len(lines)} metric lines")

    if dry_run:
        print("\n  Sample lines (first 10):")
        for line in lines[:10]:
            print(f"    {line}")
        if len(lines) > 10:
            print(f"    ... and {len(lines) - 10} more")
        return True

    # Import to VictoriaMetrics
    try:
        url = f"{VICTORIA_METRICS_URL}/api/v1/import/prometheus"
        data = '\n'.join(lines)

        response = requests.post(
            url,
            data=data.encode('utf-8'),
            headers={'Content-Type': 'text/plain'},
            timeout=60
        )
        response.raise_for_status()
        print(f"  Successfully imported {len(lines)} metric points")
        return True

    except Exception as e:
        print(f"  Error importing data: {e}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"  Response: {e.response.text[:500]}")
        return False


def main():
    parser = argparse.ArgumentParser(
        description='Re-import Shotgun event metrics with original timestamps',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  # List all events
  python reimport_event.py --list

  # Re-import specific event (dry run)
  python reimport_event.py --event 123456 --dry-run

  # Re-import specific event
  python reimport_event.py --event 123456

  # Re-import all events
  python reimport_event.py --all
        '''
    )

    parser.add_argument('--list', action='store_true', help='List all events')
    parser.add_argument('--event', type=int, help='Event ID to re-import')
    parser.add_argument('--all', action='store_true', help='Re-import all events')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be done without actually doing it')
    parser.add_argument('--db', type=str, default=str(DB_FILE), help=f'Path to SQLite database (default: {DB_FILE})')

    args = parser.parse_args()

    # Check database exists
    db_path = Path(args.db)
    if not db_path.exists():
        print(f"Error: Database not found at {db_path}")
        sys.exit(1)

    conn = sqlite3.connect(db_path)

    try:
        if args.list:
            list_events(conn)
            return

        if not args.event and not args.all:
            parser.print_help()
            print("\nError: Must specify --list, --event, or --all")
            sys.exit(1)

        events_to_process = []

        if args.all:
            all_events = get_all_events(conn)
            events_to_process = [(event_id, event_name) for event_id, event_name, _ in all_events]
            print(f"\nProcessing {len(events_to_process)} events...")
        else:
            # Verify event exists
            cursor = conn.cursor()
            cursor.execute('SELECT COUNT(*) FROM tickets WHERE event_id = ?', (args.event,))
            count = cursor.fetchone()[0]
            if count == 0:
                print(f"Error: Event {args.event} not found in database")
                sys.exit(1)
            event_name = get_event_name(conn, args.event)
            events_to_process = [(args.event, event_name)]

        # Process each event
        success_count = 0
        for event_id, event_name in events_to_process:
            print(f"\n{'='*80}")
            print(f"Processing event: {event_id} - {event_name}")
            print('='*80)

            # Delete existing metrics
            if delete_event_metrics(event_id, event_name, dry_run=args.dry_run):
                # Re-import with original timestamps
                if reimport_event_data(conn, event_id, dry_run=args.dry_run):
                    success_count += 1

        print(f"\n{'='*80}")
        if args.dry_run:
            print(f"[DRY RUN] Would process {success_count}/{len(events_to_process)} events successfully")
        else:
            print(f"Successfully processed {success_count}/{len(events_to_process)} events")
        print('='*80)

    finally:
        conn.close()


if __name__ == '__main__':
    main()
