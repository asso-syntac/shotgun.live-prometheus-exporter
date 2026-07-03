#!/usr/bin/env python3
"""
List the latest ticket sales recorded in the local SQLite database.

Examples:
  # 20 latest sales
  python sales_cli.py

  # 50 latest sales for a given event (name substring or event ID)
  python sales_cli.py --event foreztival -n 50
  python sales_cli.py --event 494247

  # Filter by ticket name
  python sales_cli.py --ticket "early bird"

  # Only refunded/canceled tickets since a date
  python sales_cli.py --status refunded --since 2026-01-01

  # Machine-readable output
  python sales_cli.py --json
"""

import sys
import json
import sqlite3
import argparse
from pathlib import Path

STATUSES = ['valid', 'refunded', 'canceled', 'resold']


def default_db_path() -> Path:
    """Prefer the container path, fall back to ./data next to this script."""
    container_path = Path('/data/shotgun_tickets.db')
    if container_path.exists():
        return container_path
    return Path(__file__).resolve().parent / 'data' / 'shotgun_tickets.db'


def open_db(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(f'file:{db_path}?mode=ro', uri=True)
    # SQLite LIKE/lower() are ASCII-only; use Python casefold for accents
    conn.create_function(
        'contains_ci', 2,
        lambda haystack, needle: 1 if haystack and needle.casefold() in haystack.casefold() else 0
    )
    return conn


def build_query(args) -> tuple:
    where = []
    params = []

    if args.event:
        if args.event.isdigit():
            where.append('t.event_id = ?')
            params.append(int(args.event))
        else:
            where.append('contains_ci(e.event_name, ?)')
            params.append(args.event)

    if args.ticket:
        where.append('contains_ci(t.deal_title, ?)')
        params.append(args.ticket)

    if args.status != 'all':
        where.append('t.ticket_status = ?')
        params.append(args.status)

    if args.since:
        where.append('t.ordered_at >= ?')
        params.append(args.since)

    query = '''
        SELECT t.ordered_at, t.ticket_status,
               COALESCE(e.event_name, 'Unknown Event') AS event_name, t.event_id,
               COALESCE(t.deal_title, 'Unknown Ticket') AS ticket_title,
               t.deal_price, COALESCE(t.deal_channel, 'unknown') AS channel
        FROM tickets t
        LEFT JOIN events_cache e ON e.event_id = t.event_id
    '''
    if where:
        query += ' WHERE ' + ' AND '.join(where)
    query += ' ORDER BY t.ordered_at DESC LIMIT ?'
    params.append(args.limit)

    return query, params


def list_events(conn: sqlite3.Connection):
    cursor = conn.cursor()
    cursor.execute('''
        SELECT t.event_id, COALESCE(e.event_name, 'Unknown Event'),
               COUNT(*), MAX(t.ordered_at)
        FROM tickets t
        LEFT JOIN events_cache e ON e.event_id = t.event_id
        GROUP BY t.event_id
        ORDER BY MAX(t.ordered_at) DESC
    ''')
    rows = cursor.fetchall()

    if not rows:
        print("No events found in database")
        return

    print(f"{'EVENT ID':<10} {'TICKETS':>8}  {'LAST SALE':<19}  EVENT NAME")
    print('-' * 90)
    for event_id, event_name, count, last_sale in rows:
        last_sale = (last_sale or '')[:19]
        print(f"{event_id:<10} {count:>8}  {last_sale:<19}  {event_name}")
    print(f"\n{len(rows)} event(s)")


def truncate(text: str, width: int) -> str:
    return text if len(text) <= width else text[:width - 1] + '…'


def print_table(rows):
    if not rows:
        print("No sales found matching the given filters")
        return

    event_w = min(max(len(r[2]) for r in rows), 38)
    ticket_w = min(max(len(r[4]) for r in rows), 30)

    header = (f"{'ORDERED AT':<19}  {'STATUS':<8}  {'EVENT':<{event_w}}  "
              f"{'TICKET':<{ticket_w}}  {'PRICE':>9}  CHANNEL")
    print(header)
    print('-' * len(header))

    total_cents = 0
    for ordered_at, status, event_name, _event_id, ticket_title, price_cents, channel in rows:
        price_cents = price_cents or 0
        total_cents += price_cents
        print(f"{(ordered_at or '')[:19]:<19}  {status:<8}  "
              f"{truncate(event_name, event_w):<{event_w}}  "
              f"{truncate(ticket_title, ticket_w):<{ticket_w}}  "
              f"{price_cents / 100:>8.2f}€  {channel}")

    print(f"\n{len(rows)} ticket(s) — total {total_cents / 100:.2f}€")


def print_json(rows):
    sales = [
        {
            'ordered_at': ordered_at,
            'status': status,
            'event_id': event_id,
            'event_name': event_name,
            'ticket_title': ticket_title,
            'price_euros': (price_cents or 0) / 100,
            'channel': channel,
        }
        for ordered_at, status, event_name, event_id, ticket_title, price_cents, channel in rows
    ]
    print(json.dumps(sales, ensure_ascii=False, indent=2))


def main():
    parser = argparse.ArgumentParser(
        description='List the latest ticket sales from the local database',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__.split('Examples:')[1] if 'Examples:' in __doc__ else None
    )
    parser.add_argument('-n', '--limit', type=int, default=20,
                        help='Number of sales to show (default: 20)')
    parser.add_argument('--event', type=str,
                        help='Filter by event: name substring (case-insensitive) or event ID')
    parser.add_argument('--ticket', type=str,
                        help='Filter by ticket name substring (case-insensitive)')
    parser.add_argument('--status', type=str, default='all',
                        choices=STATUSES + ['all'],
                        help='Filter by ticket status (default: all)')
    parser.add_argument('--since', type=str, metavar='YYYY-MM-DD',
                        help='Only show sales ordered on or after this date')
    parser.add_argument('--json', action='store_true', help='Output as JSON')
    parser.add_argument('--list-events', action='store_true',
                        help='List known events with ticket counts and exit')
    parser.add_argument('--db', type=str, default=str(default_db_path()),
                        help='Path to SQLite database (default: %(default)s)')

    args = parser.parse_args()

    db_path = Path(args.db)
    if not db_path.exists():
        print(f"Error: Database not found at {db_path}")
        sys.exit(1)

    conn = open_db(db_path)
    try:
        if args.list_events:
            list_events(conn)
            return

        query, params = build_query(args)
        rows = conn.execute(query, params).fetchall()

        if args.json:
            print_json(rows)
        else:
            print_table(rows)
    finally:
        conn.close()


if __name__ == '__main__':
    main()
