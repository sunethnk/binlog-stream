#!/bin/bash
# MySQL CDC Initialization Script
# This runs automatically when the MySQL container starts

set -e

echo "========================================="
echo "Initializing MySQL for CDC Testing"
echo "========================================="

# Wait for MySQL to be ready
until mysql -uroot -prootpass -e "SELECT 1" &>/dev/null; do
    echo "Waiting for MySQL to be ready..."
    sleep 2
done

echo "Creating replication user..."
mysql -uroot -prootpass <<EOF
-- Create replication user
CREATE USER IF NOT EXISTS 'repl_user'@'%' IDENTIFIED BY 'replpass';
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'repl_user'@'%';
GRANT SELECT ON testdb.* TO 'repl_user'@'%';
FLUSH PRIVILEGES;

-- Use test database
USE testdb;

-- Create test tables
DROP TABLE IF EXISTS users;
CREATE TABLE users (
    id INT PRIMARY KEY AUTO_INCREMENT,
    username VARCHAR(50) NOT NULL,
    email VARCHAR(100),
    status ENUM('active', 'inactive', 'pending') DEFAULT 'active',
    balance DECIMAL(10,2) DEFAULT 0.00,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_username (username),
    INDEX idx_email (email)
);

DROP TABLE IF EXISTS orders;
CREATE TABLE orders (
    order_id INT PRIMARY KEY AUTO_INCREMENT,
    user_id INT NOT NULL,
    product_name VARCHAR(100),
    quantity INT DEFAULT 1,
    total_amount DECIMAL(10,2),
    order_status VARCHAR(20) DEFAULT 'pending',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(id)
);

DROP TABLE IF EXISTS events;
CREATE TABLE events (
    event_id BIGINT PRIMARY KEY AUTO_INCREMENT,
    event_type VARCHAR(50),
    event_data JSON,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert sample data
INSERT INTO users (username, email, status, balance) VALUES
    ('alice', 'alice@example.com', 'active', 1000.00),
    ('bob', 'bob@example.com', 'active', 500.50),
    ('charlie', 'charlie@example.com', 'inactive', 0.00);

INSERT INTO orders (user_id, product_name, quantity, total_amount) VALUES
    (1, 'Laptop', 1, 999.99),
    (1, 'Mouse', 2, 29.98),
    (2, 'Keyboard', 1, 79.99);

INSERT INTO events (event_type, event_data) VALUES
    ('user_login', '{"user_id": 1, "ip": "192.168.1.100"}'),
    ('order_placed', '{"order_id": 1, "amount": 999.99}');

-- Show current binlog position
SHOW MASTER STATUS;

-- Show tables
SHOW TABLES;

EOF

echo "✓ MySQL initialized successfully!"
echo ""
echo "Connection details:"
echo "  Host: localhost:3306"
echo "  Database: testdb"
echo "  Root user: root / rootpass"
echo "  Replication user: repl_user / replpass"
echo ""
echo "Tables created: users, orders, events"
echo "Sample data inserted"
echo "========================================="
