# test_publisher.py
# Simple test publisher for python_publisher.so

stats = {
    'total': 0,
    'by_table': {}
}

def on_event(event):
    """Main event handler (called for each CDC event)"""
    stats['total'] += 1
    
    # Track events by table
    table_key = f"{event.get('db', 'unknown')}.{event.get('table', 'unknown')}"
    stats['by_table'][table_key] = stats['by_table'].get(table_key, 0) + 1
    
    # Print event details
    print(f"=== Event #{stats['total']} ===")
    print(f"Transaction: {event.get('txn', 'N/A')}")
    print(f"Database: {event.get('db', 'N/A')}")
    print(f"Table: {event.get('table', 'N/A')}")
    print(f"JSON: {event.get('json', 'N/A')}")
    print("=" * 30)
    
    return 0  # Success

def on_init(config):
    """Optional: Called on init"""
    print("[Python] Initializing test publisher")
    print("[Python] Configuration received:")
    for k, v in config.items():
        print(f"  {k} = {v}")
    return 0

def on_start():
    """Optional: Called on start"""
    print("[Python] Starting test publisher")
    stats['total'] = 0
    stats['by_table'] = {}
    return 0

def on_stop():
    """Optional: Called on stop"""
    print("[Python] Stopping test publisher")
    print(f"[Python] Total events processed: {stats['total']}")
    print("[Python] Events by table:")
    for table_name, count in stats['by_table'].items():
        print(f"  {table_name}: {count}")
    return 0

def on_cleanup():
    """Optional: Called on cleanup"""
    print("[Python] Cleanup complete")

def on_health():
    """Optional: Called for health check"""
    print(f"[Python] Health check: {stats['total']} events processed")
    return 0  # Healthy
