#!/bin/bash
# PostgreSQL CDC Initialization Script
# This runs automatically when the PostgreSQL container starts

set -e

echo "========================================="
echo "Initializing PostgreSQL for CDC Testing"
echo "========================================="

# Create replication user
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    -- Create replication user
    CREATE USER repl_user WITH REPLICATION LOGIN PASSWORD 'replpass';
    GRANT CONNECT ON DATABASE testdb TO repl_user;
    GRANT USAGE ON SCHEMA public TO repl_user;
    GRANT SELECT ON ALL TABLES IN SCHEMA public TO repl_user;
    ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO repl_user;

    -- Create test tables
    DROP TABLE IF EXISTS events CASCADE;
    DROP TABLE IF EXISTS orders CASCADE;
    DROP TABLE IF EXISTS users CASCADE;

    CREATE TABLE users (
        id SERIAL PRIMARY KEY,
        username VARCHAR(50) NOT NULL,
        email VARCHAR(100),
        status VARCHAR(20) DEFAULT 'active',
        balance NUMERIC(10,2) DEFAULT 0.00,
        metadata JSONB,
        created_at TIMESTAMP DEFAULT NOW(),
        updated_at TIMESTAMP DEFAULT NOW()
    );

    CREATE INDEX idx_users_username ON users(username);
    CREATE INDEX idx_users_email ON users(email);

    CREATE TABLE orders (
        order_id SERIAL PRIMARY KEY,
        user_id INT NOT NULL,
        product_name VARCHAR(100),
        quantity INT DEFAULT 1,
        total_amount NUMERIC(10,2),
        order_status VARCHAR(20) DEFAULT 'pending',
        created_at TIMESTAMP DEFAULT NOW(),
        FOREIGN KEY (user_id) REFERENCES users(id)
    );

    CREATE TABLE events (
        event_id BIGSERIAL PRIMARY KEY,
        event_type VARCHAR(50),
        event_data JSONB,
        created_at TIMESTAMP DEFAULT NOW()
    );

    -- Set REPLICA IDENTITY FULL for all tables (needed for UPDATE/DELETE to show full row)
    ALTER TABLE users REPLICA IDENTITY FULL;
    ALTER TABLE orders REPLICA IDENTITY FULL;
    ALTER TABLE events REPLICA IDENTITY FULL;

    -- Insert sample data
    INSERT INTO users (username, email, status, balance, metadata) VALUES
        ('alice', 'alice@example.com', 'active', 1000.00, '{"vip": true, "level": 5}'),
        ('bob', 'bob@example.com', 'active', 500.50, '{"vip": false, "level": 2}'),
        ('charlie', 'charlie@example.com', 'inactive', 0.00, '{"vip": false, "level": 1}');

    INSERT INTO orders (user_id, product_name, quantity, total_amount) VALUES
        (1, 'Laptop', 1, 999.99),
        (1, 'Mouse', 2, 29.98),
        (2, 'Keyboard', 1, 79.99);

    INSERT INTO events (event_type, event_data) VALUES
        ('user_login', '{"user_id": 1, "ip": "192.168.1.100"}'),
        ('order_placed', '{"order_id": 1, "amount": 999.99}');

    -- Create publication for CDC
    CREATE PUBLICATION cdc_publication FOR ALL TABLES;

    -- Create replication slot
    SELECT pg_create_logical_replication_slot('cdc_slot', 'pgoutput');

    -- Show current WAL position
    SELECT pg_current_wal_lsn();

    -- Show tables
    SELECT table_name FROM information_schema.tables 
    WHERE table_schema = 'public' 
    ORDER BY table_name;

    -- Show publication info
    SELECT * FROM pg_publication_tables WHERE pubname = 'cdc_publication';

    -- Show replication slot
    SELECT slot_name, plugin, slot_type, database, active 
    FROM pg_replication_slots 
    WHERE slot_name = 'cdc_slot';

EOSQL

echo "✓ PostgreSQL initialized successfully!"
echo ""
echo "Connection details:"
echo "  Host: localhost:5432"
echo "  Database: testdb"
echo "  Root user: postgres / rootpass"
echo "  Replication user: repl_user / replpass"
echo ""
echo "Tables created: users, orders, events"
echo "Sample data inserted"
echo "Replication slot: cdc_slot"
echo "Publication: cdc_publication"
echo "========================================="
