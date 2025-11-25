#!/usr/bin/env python3
"""
zmq_subscriber.py
Simple ZMQ subscriber to test binlog streaming events

Usage:
    python3 zmq_subscriber.py [endpoint]
    
Default endpoint: tcp://localhost:5557
"""

import zmq
import json
import sys
from datetime import datetime

def main():
    # Get endpoint from command line or use default
    endpoint = sys.argv[1] if len(sys.argv) > 1 else "tcp://localhost:5557"
    
    print("=" * 60)
    print("ZMQ Binlog Event Subscriber")
    print("=" * 60)
    print(f"Connecting to: {endpoint}")
    print("Press Ctrl+C to stop")
    print("=" * 60)
    print()
    
    # Create ZMQ context and subscriber socket
    context = zmq.Context()
    socket = context.socket(zmq.SUB)
    
    # Connect to the publisher
    socket.connect(endpoint)
    
    # Subscribe to all messages (empty string = all)
    socket.setsockopt_string(zmq.SUBSCRIBE, "")
    
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Waiting for events...")
    print()
    
    event_count = 0
    
    try:
        while True:
            # Receive message
            message = socket.recv_string()
            event_count += 1
            
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            # Try to parse as JSON and pretty print
            try:
                event = json.loads(message)
                
                # Print header
                print(f"[{timestamp}] Event #{event_count}")
                print("-" * 60)
                
                # Print event type and metadata
                event_type = event.get('type', 'UNKNOWN')
                txn = event.get('txn', 'N/A')
                db = event.get('db', 'N/A')
                table = event.get('table', 'N/A')
                
                print(f"Type: {event_type}")
                print(f"Transaction: {txn[:8]}...")  # Show first 8 chars of UUID
                print(f"Database: {db}")
                
                if table != 'N/A':
                    print(f"Table: {table}")
                
                # Print rows for DML events
                if event_type in ['INSERT', 'UPDATE', 'DELETE']:
                    rows = event.get('rows', [])
                    print(f"Rows: {len(rows)}")
                    
                    for i, row in enumerate(rows[:3], 1):  # Show first 3 rows
                        print(f"\n  Row {i}:")
                        if event_type == 'UPDATE':
                            print(f"    Before: {json.dumps(row.get('before', {}), indent=6)}")
                            print(f"    After:  {json.dumps(row.get('after', {}), indent=6)}")
                        else:
                            print(f"    {json.dumps(row, indent=6)}")
                    
                    if len(rows) > 3:
                        print(f"  ... and {len(rows) - 3} more row(s)")
                
                # Print query for DDL events
                elif event_type.startswith('DDL'):
                    query = event.get('query', 'N/A')
                    print(f"Query: {query[:100]}")  # Show first 100 chars
                
                print()
                print("Raw JSON:")
                print(json.dumps(event, indent=2))
                print()
                
            except json.JSONDecodeError:
                # Not JSON, just print raw message
                print(f"[{timestamp}] Event #{event_count} (Raw):")
                print(message)
                print()
            
    except KeyboardInterrupt:
        print()
        print("=" * 60)
        print(f"Stopped. Received {event_count} event(s)")
        print("=" * 60)
    
    finally:
        socket.close()
        context.term()

if __name__ == "__main__":
    main()
