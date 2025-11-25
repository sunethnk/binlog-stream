#!/usr/bin/env python3
"""
redis_stream_monitor.py
Monitor CDC events from Redis Streams in real-time

Install: pip3 install redis

Usage:
    python3 redis_stream_monitor.py

Press Ctrl+C to stop
"""

import redis
import json
import sys
from datetime import datetime
import time


class StreamMonitor:
    def __init__(self, host='localhost', port=6379, db=0, start_from='0-0'):
        """
        start_from:
            '0-0'  -> read everything from the beginning for newly-discovered streams
            '$'    -> only read new events (skip existing backlog)
        """
        self.r = redis.Redis(host=host, port=port, db=db, decode_responses=True)
        self.streams = {}  # stream_name -> last_id
        self.start_from = start_from
        self.stats = {
            'total': 0,
            'inserts': 0,
            'updates': 0,
            'deletes': 0,
            'commits': 0
        }

    def discover_streams(self):
        """Find all CDC streams and register them if new."""
        # Support both "cdc:*" and "cdc.*" naming styles
        stream_names = set()

        # KEYS is fine for dev/small keyspaces; for big ones use SCAN
        for pattern in ('cdc:*', 'cdc.*'):
            try:
                for key in self.r.keys(pattern):
                    stream_type = self.r.type(key)
                    if stream_type == 'stream':
                        stream_names.add(key)
            except redis.RedisError:
                continue

        new_streams = 0
        for stream in stream_names:
            if stream not in self.streams:
                # Start from beginning to avoid missing earlier commits
                self.streams[stream] = self.start_from
                new_streams += 1

        return len(stream_names), new_streams

    def format_timestamp(self, event_id):
        """Convert Redis stream ID to readable timestamp."""
        try:
            timestamp_ms = int(event_id.split('-')[0])
            return datetime.fromtimestamp(timestamp_ms / 1000).strftime('%H:%M:%S')
        except Exception:
            return "??:??:??"

    def display_event(self, stream_name, event_id, data):
        """Display event in a nice format."""
        timestamp = self.format_timestamp(event_id)

        # Safely get the JSON payload
        raw_json = data.get('json')
        if raw_json is None:
            # If publisher used a different field, fall back to first value
            if data:
                raw_json = list(data.values())[0]
            else:
                raw_json = "{}"

        try:
            event_json = json.loads(raw_json)
        except json.JSONDecodeError:
            event_json = {}
        
        event_type = event_json.get('type', 'UNKNOWN')

        # Normalize for safety
        et_upper = str(event_type).upper()

        # Update statistics
        self.stats['total'] += 1
        if et_upper == 'INSERT':
            self.stats['inserts'] += 1
            icon = '✅'
        elif et_upper == 'UPDATE':
            self.stats['updates'] += 1
            icon = '🔄'
        elif et_upper == 'DELETE':
            self.stats['deletes'] += 1
            icon = '🗑️'
        elif et_upper == 'COMMIT':
            self.stats['commits'] += 1
            icon = '💾'
        else:
            icon = '📩'

        db = data.get('db', event_json.get('db', '?'))
        table = data.get('table', event_json.get('table', '?'))
        txn = data.get('txn', event_json.get('txn', 'none'))

        rows_obj = event_json.get('rows', [])
        try:
            rows = len(rows_obj)
        except TypeError:
            rows = 0

        print(
            f"[{timestamp}] {icon} {et_upper:6s} "
            f"{db}.{(table or ''):20s} "
            f"rows={rows:2d} txn={str(txn)[:32]} stream={stream_name} "
            f"data={event_json}"
        )

    def show_stats(self):
        """Show statistics."""
        print("\n" + "=" * 80)
        print("📊 Statistics:")
        print(f"   Total: {self.stats['total']} events")
        print(f"   ✅ INSERTs: {self.stats['inserts']}")
        print(f"   🔄 UPDATEs: {self.stats['updates']}")
        print(f"   🗑️  DELETEs: {self.stats['deletes']}")
        print(f"   💾 COMMITs: {self.stats['commits']}")
        print("=" * 80)

    def monitor(self):
        """Monitor streams continuously."""
        print("🔍 CDC Stream Monitor")
        print("=" * 80)

        # Initial discovery
        total_streams, new_streams = self.discover_streams()

        if total_streams == 0:
            print("⚠️  No CDC streams found (pattern: cdc:* / cdc.*)")
            print("   Make sure redis_publisher is running and publishing events")
            print("\n   Waiting for streams to appear...")

            while not self.streams:
                time.sleep(2)
                self.discover_streams()

        print(f"Monitoring {len(self.streams)} stream(s):")
        for stream in sorted(self.streams.keys()):
            print(f"  • {stream}")

        print("\nWaiting for events... (Ctrl+C to stop)")
        print("-" * 80)

        last_discovery = time.time()

        try:
            while True:
                now = time.time()
                # Rediscover streams every 5 seconds
                if now - last_discovery >= 5:
                    total_streams, new_streams = self.discover_streams()
                    if new_streams > 0:
                        print(
                            json.dumps(
                                {
                                    "event": "stream_discovery",
                                    "total_streams": total_streams,
                                    "new_streams": new_streams,
                                    "streams": list(self.streams.keys()),
                                }
                            )
                        )
                    last_discovery = now

                if not self.streams:
                    time.sleep(1)
                    continue

                # Read from all streams.
                # block=1000 means wait up to 1 second for new events.
                try:
                    events = self.r.xread(self.streams, count=50, block=1000)
                except redis.RedisError as e:
                    print(
                        json.dumps(
                            {
                                "event": "error",
                                "type": "xread",
                                "error": str(e),
                            }
                        )
                    )
                    time.sleep(1)
                    continue

                if events:
                    for stream_name, messages in events:
                        for message_id, data in messages:
                            # Update last seen ID for this stream
                            self.streams[stream_name] = message_id
                            # Display event
                            self.display_event(stream_name, message_id, data)

        except KeyboardInterrupt:
            print("\n\n🛑 Monitoring stopped")
            self.show_stats()


def main():
    # Check for help
    if len(sys.argv) > 1 and sys.argv[1] in ['-h', '--help']:
        print(__doc__)
        return

    # Connect to Redis
    try:
        r = redis.Redis(host='localhost', port=6379, db=0)
        r.ping()
        print("✅ Connected to Redis\n")
    except redis.ConnectionError:
        print("❌ Cannot connect to Redis")
        print("   Make sure Redis is running: redis-server")
        sys.exit(1)

    # Start monitoring
    monitor = StreamMonitor(start_from='0-0')  # start_from='$' if you only care about new events
    monitor.monitor()


if __name__ == '__main__':
    main()
