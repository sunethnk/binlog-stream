#!/usr/bin/env python3
"""
redis_pubsub_monitor.py
Monitor CDC events from Redis Pub/Sub channels in real-time

Install: pip3 install redis

Usage:
    python3 redis_pubsub_monitor.py                  # Monitor default channel
    python3 redis_pubsub_monitor.py cdc_events       # Monitor specific channel
    python3 redis_pubsub_monitor.py "cdc*"           # Pattern subscription
    
Press Ctrl+C to stop
"""

import redis
import json
import sys
from datetime import datetime

class PubSubMonitor:
    def __init__(self, host='localhost', port=6379, db=0):
        self.r = redis.Redis(host=host, port=port, db=db, decode_responses=True)
        self.pubsub = self.r.pubsub()
        self.stats = {
            'total': 0,
            'inserts': 0,
            'updates': 0,
            'deletes': 0,
            'commits': 0,
            'start_time': datetime.now()
        }
    
    def display_event(self, message):
        """Display event in a nice format"""
        try:
            event = json.loads(message)
            event_type = event.get('type', 'UNKNOWN')
            
            # Update statistics
            self.stats['total'] += 1
            if event_type == 'INSERT':
                self.stats['inserts'] += 1
                icon = '✅'
            elif event_type == 'UPDATE':
                self.stats['updates'] += 1
                icon = '🔄'
            elif event_type == 'DELETE':
                self.stats['deletes'] += 1
                icon = '🗑️'
            elif event_type == 'COMMIT':
                self.stats['commits'] += 1
                icon = '💾'
            else:
                icon = '📩'
            
            timestamp = datetime.now().strftime('%H:%M:%S')
            db = event.get('db', '?')
            table = event.get('table', '?')
            txn = event.get('txn', 'none')
            rows = len(event.get('rows', []))
            
            print(f"[{timestamp}] {icon} {event_type:6s} {db}.{table:20s} "
                  f"rows={rows:2d} txn={txn[:32]} data={event}")
            
        except json.JSONDecodeError:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] ⚠️  Non-JSON message: {message[:50]}")
        except Exception as e:
            print(f"Error processing message: {e}")
    
    def show_stats(self):
        """Show statistics"""
        duration = (datetime.now() - self.stats['start_time']).total_seconds()
        
        print("\n" + "="*80)
        print("📊 Statistics:")
        print(f"   Duration: {duration:.1f} seconds")
        print(f"   Total: {self.stats['total']} events")
        
        if self.stats['total'] > 0:
            rate = self.stats['total'] / duration if duration > 0 else 0
            print(f"   Rate: {rate:.2f} events/sec")
        
        print(f"   ✅ INSERTs: {self.stats['inserts']}")
        print(f"   🔄 UPDATEs: {self.stats['updates']}")
        print(f"   🗑️  DELETEs: {self.stats['deletes']}")
        print(f"   💾 COMMITs: {self.stats['commits']}")
        print("="*80)
    
    def check_channels(self):
        """Check active channels"""
        channels = self.r.pubsub_channels()
        
        print("\n📡 Active Pub/Sub Channels:")
        if channels:
            for channel in sorted(channels):
                numsub = self.r.pubsub_numsub(channel)
                subscribers = numsub[0][1] if numsub else 0
                print(f"   • {channel} ({subscribers} subscribers)")
        else:
            print("   (No active channels found)")
        
        return len(channels)
    
    def monitor(self, channel='cdc_events', use_pattern=False):
        """Monitor channel(s) continuously"""
        print("📡 Redis Pub/Sub Monitor")
        print("="*80)
        
        # Check connection
        try:
            self.r.ping()
            print("✅ Connected to Redis\n")
        except redis.ConnectionError:
            print("❌ Cannot connect to Redis")
            sys.exit(1)
        
        # Check active channels
        self.check_channels()
        
        # Subscribe
        if use_pattern:
            print(f"\n🔍 Subscribing to pattern: {channel}")
            self.pubsub.psubscribe(channel)
        else:
            print(f"\n🔍 Subscribing to channel: {channel}")
            self.pubsub.subscribe(channel)
        
        print("\nWaiting for messages... (Ctrl+C to stop)")
        print("-"*80)
        
        try:
            for message in self.pubsub.listen():
                if message['type'] == 'message':
                    self.display_event(message['data'])
                elif message['type'] == 'pmessage':
                    # Pattern subscription message
                    print(f"[{datetime.now().strftime('%H:%M:%S')}] "
                          f"📨 From channel: {message['channel']}")
                    self.display_event(message['data'])
                elif message['type'] == 'subscribe':
                    print(f"✅ Subscribed to: {message['channel']}")
                elif message['type'] == 'psubscribe':
                    print(f"✅ Pattern subscribed to: {message['pattern']}")
                
        except KeyboardInterrupt:
            print("\n\n🛑 Monitoring stopped")
            self.pubsub.unsubscribe()
            self.show_stats()

def main():
    # Parse arguments
    if len(sys.argv) > 1:
        if sys.argv[1] in ['-h', '--help']:
            print(__doc__)
            return
        
        channel = sys.argv[1]
        # Check if it's a pattern (contains * or ?)
        use_pattern = '*' in channel or '?' in channel
    else:
        channel = 'cdc_events'
        use_pattern = False
    
    # Start monitoring
    monitor = PubSubMonitor()
    monitor.monitor(channel, use_pattern)

if __name__ == '__main__':
    main()
