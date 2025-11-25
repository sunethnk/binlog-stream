#!/usr/bin/env python3
"""
webhook_receiver.py
Simple Flask server to receive and test CDC webhook events

Install dependencies:
    pip install flask

Run:
    python3 webhook_receiver.py

Then configure webhook_publisher.so with:
    "webhook_url": "http://localhost:5000/webhook/cdc"
"""

from flask import Flask, request, jsonify
import json
from datetime import datetime
import logging

app = Flask(__name__)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Statistics
stats = {
    'total_received': 0,
    'inserts': 0,
    'updates': 0,
    'deletes': 0,
    'errors': 0,
    'last_event': None
}

@app.route('/webhook/cdc', methods=['POST'])
def receive_cdc_event():
    """
    Receive CDC events from webhook publisher
    """
    stats['total_received'] += 1
    
    try:
        # Get JSON payload
        event = request.get_json()
        
        if not event:
            logger.error("Empty payload received")
            stats['errors'] += 1
            return jsonify({'status': 'error', 'message': 'Empty payload'}), 400
        
        # Extract event details
        event_type = event.get('type', 'UNKNOWN')
        db = event.get('db', 'unknown')
        table = event.get('table', 'unknown')
        txn = event.get('txn', 'none')
        rows = event.get('rows', [])
        
        logger.info(f"📩 Received {event_type}: {db}.{table} (txn={txn}, data={event})")

        # Update statistics
        if event_type == 'INSERT':
            stats['inserts'] += 1
        elif event_type == 'UPDATE':
            stats['updates'] += 1
        elif event_type == 'DELETE':
            stats['deletes'] += 1
        
        stats['last_event'] = {
            'type': event_type,
            'db': db,
            'table': table,
            'time': datetime.now().isoformat()
        }
        
        # Log the event
        logger.info(f"📩 Received {event_type}: {db}.{table} (txn={txn}, rows={len(rows)})")
        
        # Process based on event type
        if event_type == 'INSERT':
            handle_insert(db, table, rows, txn)
        elif event_type == 'UPDATE':
            handle_update(db, table, rows, txn)
        elif event_type == 'DELETE':
            handle_delete(db, table, rows, txn)
        elif event_type == 'COMMIT':
            handle_commit(db, txn)
        else:
            logger.warning(f"Unknown event type: {event_type}")
        
        # Return success
        return jsonify({
            'status': 'success',
            'message': f'Event {event_type} processed',
            'txn': txn
        }), 200
        
    except Exception as e:
        logger.error(f"❌ Error processing event: {e}")
        stats['errors'] += 1
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500

def handle_insert(db, table, rows, txn):
    """Handle INSERT events"""
    logger.info(f"  ✅ INSERT into {db}.{table}: {len(rows)} row(s)")
    for i, row in enumerate(rows):
        logger.debug(f"    Row {i+1}: {json.dumps(row)}")
    
    # YOUR CUSTOM LOGIC HERE
    # Example: Send notification, update cache, trigger workflow, etc.
    if table == 'radacct':
        # Example: New radius session started
        username = row.get('username') if rows else None
        if username:
            logger.info(f"    🆕 New radius session for: {username}")
    
def handle_update(db, table, rows, txn):
    """Handle UPDATE events"""
    logger.info(f"  🔄 UPDATE in {db}.{table}: {len(rows)} row(s)")
    for i, row in enumerate(rows):
        before = row.get('before', {})
        after = row.get('after', {})
        
        # Find changed fields
        changed = []
        for key in after:
            if key in before and before[key] != after[key]:
                changed.append(f"{key}: {before[key]} → {after[key]}")
        
        if changed:
            logger.debug(f"    Row {i+1} changes: {', '.join(changed)}")
    
    # YOUR CUSTOM LOGIC HERE
    # Example: Invalidate cache, send update notification, etc.

def handle_delete(db, table, rows, txn):
    """Handle DELETE events"""
    logger.info(f"  🗑️  DELETE from {db}.{table}: {len(rows)} row(s)")
    for i, row in enumerate(rows):
        logger.debug(f"    Deleted row {i+1}: {json.dumps(row)}")
    
    # YOUR CUSTOM LOGIC HERE
    # Example: Clean up related data, send notification, etc.

def handle_commit(db, txn):
    """Handle COMMIT events"""
    logger.info(f"  💾 COMMIT transaction: {txn}")

@app.route('/webhook/stats', methods=['GET'])
def get_stats():
    """Get statistics"""
    return jsonify(stats), 200

@app.route('/webhook/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({
        'status': 'healthy',
        'uptime': 'running',
        'stats': stats
    }), 200

@app.route('/', methods=['GET'])
def index():
    """Home page with instructions"""
    html = f"""
    <html>
    <head>
        <title>CDC Webhook Receiver</title>
        <style>
            body {{ font-family: Arial, sans-serif; margin: 40px; }}
            h1 {{ color: #333; }}
            .stats {{ background: #f0f0f0; padding: 20px; border-radius: 5px; }}
            .event {{ background: #e8f5e9; padding: 10px; margin: 10px 0; border-radius: 3px; }}
            code {{ background: #f5f5f5; padding: 2px 5px; border-radius: 3px; }}
        </style>
    </head>
    <body>
        <h1>🔗 CDC Webhook Receiver</h1>
        <p>Webhook endpoint is ready to receive CDC events!</p>
        
        <h2>📊 Statistics</h2>
        <div class="stats">
            <p><strong>Total Events:</strong> {stats['total_received']}</p>
            <p><strong>INSERTs:</strong> {stats['inserts']}</p>
            <p><strong>UPDATEs:</strong> {stats['updates']}</p>
            <p><strong>DELETEs:</strong> {stats['deletes']}</p>
            <p><strong>Errors:</strong> {stats['errors']}</p>
        </div>
        
        {f'''
        <h2>📝 Last Event</h2>
        <div class="event">
            <p><strong>Type:</strong> {stats['last_event']['type']}</p>
            <p><strong>Database:</strong> {stats['last_event']['db']}</p>
            <p><strong>Table:</strong> {stats['last_event']['table']}</p>
            <p><strong>Time:</strong> {stats['last_event']['time']}</p>
        </div>
        ''' if stats['last_event'] else '<p><em>No events received yet</em></p>'}
        
        <h2>⚙️ Configuration</h2>
        <p>Add this to your <code>config.json</code>:</p>
        <pre style="background: #f5f5f5; padding: 15px; border-radius: 5px;">{{
  "plugin": {{
    "name": "webhook",
    "library_path": "./webhook_publisher.so",
    "active": true,
    "publish_databases": ["radius", "scheduler"],
    "config": {{
      "webhook_url": "http://localhost:5000/webhook/cdc",
      "timeout_seconds": "10",
      "retry_count": "3"
    }}
  }}
}}</pre>
        
        <h2>🧪 Test Endpoints</h2>
        <ul>
            <li><code>POST /webhook/cdc</code> - Receive CDC events</li>
            <li><a href="/webhook/stats"><code>GET /webhook/stats</code></a> - View statistics</li>
            <li><a href="/webhook/health"><code>GET /webhook/health</code></a> - Health check</li>
        </ul>
        
        <h2>🔄 Refresh Stats</h2>
        <p><a href="/">Click here to refresh</a></p>
    </body>
    </html>
    """
    return html

if __name__ == '__main__':
    logger.info("🚀 Starting CDC Webhook Receiver")
    logger.info("📍 Webhook endpoint: http://localhost:5000/webhook/cdc")
    logger.info("📊 Stats endpoint: http://localhost:5000/webhook/stats")
    logger.info("💚 Health endpoint: http://localhost:5000/webhook/health")
    logger.info("🌐 Dashboard: http://localhost:5000/")
    logger.info("")
    logger.info("Waiting for CDC events...")
    
    # Run Flask server
    app.run(
        host='0.0.0.0',
        port=5000,
        debug=True
    )