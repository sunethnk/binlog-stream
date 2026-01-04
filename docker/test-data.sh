#!/bin/bash
# Test Data Generator for CDC Testing
# Uses Docker exec - no database clients required on host
# Generates DML operations on both MySQL and PostgreSQL

set -e

MYSQL_CONTAINER="cdc-mysql"
MYSQL_USER="root"
MYSQL_PASS="rootpass"
MYSQL_DB="testdb"

PG_CONTAINER="cdc-postgres"
PG_USER="postgres"
PG_DB="testdb"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

print_header() {
    echo ""
    echo -e "${BLUE}=========================================${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}=========================================${NC}"
    echo ""
}

print_test() {
    echo ""
    echo -e "${YELLOW}$1${NC}"
    echo "$(printf '%.0s-' {1..50})"
}

print_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

print_error() {
    echo -e "${RED}✗ $1${NC}"
}

# Check Docker containers
check_containers() {
    if ! docker ps | grep -q "$MYSQL_CONTAINER"; then
        print_error "MySQL container '$MYSQL_CONTAINER' is not running"
        echo "Start it with: docker-compose up -d mysql"
        exit 1
    fi

    if ! docker ps | grep -q "$PG_CONTAINER"; then
        print_error "PostgreSQL container '$PG_CONTAINER' is not running"
        echo "Start it with: docker-compose up -d postgres"
        exit 1
    fi

    print_success "Both database containers are running"
}

# Execute MySQL command
mysql_exec() {
    docker exec "$MYSQL_CONTAINER" mysql -u"$MYSQL_USER" -p"$MYSQL_PASS" "$MYSQL_DB" -e "$1" 2>/dev/null
}

# Execute PostgreSQL command
pg_exec() {
    docker exec "$PG_CONTAINER" psql -U "$PG_USER" -d "$PG_DB" -c "$1" 2>/dev/null
}

# Execute on both databases and show status
exec_both() {
    local test_name="$1"
    local mysql_sql="$2"
    local pg_sql="$3"
    
    echo "  Running on MySQL..."
    if mysql_exec "$mysql_sql"; then
        print_success "MySQL: $test_name"
    else
        print_error "MySQL: $test_name failed"
    fi
    
    echo "  Running on PostgreSQL..."
    if pg_exec "$pg_sql"; then
        print_success "PostgreSQL: $test_name"
    else
        print_error "PostgreSQL: $test_name failed"
    fi
}

# Main execution
main() {

#    rm -rf test_output/mysql/*
#    rm -rf test_output/postgres/*

    print_header "CDC Test Data Generator"
    
    check_containers
    
    # Test 1: INSERT operations
    print_test "Test 1: INSERT Operations"
    exec_both "Insert new users" \
        "INSERT INTO users (username, email, status, balance) VALUES 
            ('david', 'david@example.com', 'active', 750.00),
            ('eve', 'eve@example.com', 'pending', 250.00);" \
        "INSERT INTO users (username, email, status, balance, metadata) VALUES 
            ('david', 'david@example.com', 'active', 750.00, '{\"vip\": true, \"level\": 3}'),
            ('eve', 'eve@example.com', 'pending', 250.00, '{\"vip\": false, \"level\": 1}');"
    sleep 2
    
    # Test 2: UPDATE operations
    print_test "Test 2: UPDATE Operations"
    exec_both "Update user balances" \
        "UPDATE users SET balance = balance + 100.00 WHERE username = 'alice';" \
        "UPDATE users SET balance = balance + 100.00 WHERE username = 'alice';"
    
    exec_both "Activate inactive users" \
        "UPDATE users SET status = 'active' WHERE username = 'charlie';" \
        "UPDATE users SET status = 'active' WHERE username = 'charlie';"
    sleep 2
    
    # Test 3: Multiple INSERTs (Orders)
    print_test "Test 3: Multiple INSERTs (Orders)"
    exec_both "Create orders" \
        "INSERT INTO orders (user_id, product_name, quantity, total_amount, order_status) VALUES 
            (4, 'Monitor', 1, 299.99, 'pending'),
            (5, 'Webcam', 2, 89.98, 'pending'),
            (1, 'USB Cable', 3, 14.97, 'completed');" \
        "INSERT INTO orders (user_id, product_name, quantity, total_amount, order_status) VALUES 
            (4, 'Monitor', 1, 299.99, 'pending'),
            (5, 'Webcam', 2, 89.98, 'pending'),
            (1, 'USB Cable', 3, 14.97, 'completed');"
    sleep 2
    
    # Test 4: INSERT with JSON data
    print_test "Test 4: INSERT with JSON Data (Events)"
    exec_both "Log payment events" \
        "INSERT INTO events (event_type, event_data) VALUES 
            ('payment_processed', '{\"user_id\": 1, \"amount\": 299.99, \"method\": \"credit_card\"}'),
            ('order_shipped', '{\"order_id\": 4, \"tracking\": \"TRACK123456\"}');" \
        "INSERT INTO events (event_type, event_data) VALUES 
            ('payment_processed', '{\"user_id\": 1, \"amount\": 299.99, \"method\": \"credit_card\"}'),
            ('order_shipped', '{\"order_id\": 4, \"tracking\": \"TRACK123456\"}');"
    sleep 2
    
    # Test 5: UPDATE order status
    print_test "Test 5: UPDATE Order Status"
    exec_both "Update to processing" \
        "UPDATE orders SET order_status = 'processing' WHERE order_id IN (4, 5);" \
        "UPDATE orders SET order_status = 'processing' WHERE order_id IN (4, 5);"
    
    exec_both "Update to shipped with discount" \
        "UPDATE orders SET order_status = 'shipped', total_amount = total_amount * 0.9 WHERE order_id = 4;" \
        "UPDATE orders SET order_status = 'shipped', total_amount = total_amount * 0.9 WHERE order_id = 4;"
    sleep 2
    
    # Test 6: DELETE operations
    print_test "Test 6: DELETE Operations"
    exec_both "Delete user orders" \
        "DELETE FROM orders WHERE user_id = 3;" \
        "DELETE FROM orders WHERE user_id = 3;"
    
    exec_both "Delete inactive users" \
        "DELETE FROM users WHERE status = 'inactive';" \
        "DELETE FROM users WHERE status = 'inactive';"
    sleep 2
    
    # Test 7: Bulk operations
    print_test "Test 7: Bulk Operations (10 Events)"
    echo "  Generating bulk events..."
    for i in {1..10}; do
        mysql_exec "INSERT INTO events (event_type, event_data) VALUES 
            ('bulk_event_$i', '{\"batch\": $i, \"timestamp\": $(date +%s)}');" &>/dev/null
        
        pg_exec "INSERT INTO events (event_type, event_data) VALUES 
            ('bulk_event_$i', '{\"batch\": $i, \"timestamp\": $(date +%s)}');" &>/dev/null
        
        echo -n "."
    done
    echo ""
    print_success "MySQL: Inserted 10 bulk events"
    print_success "PostgreSQL: Inserted 10 bulk events"
    sleep 2
    
    # Test 8: Transaction simulation
    print_test "Test 8: Multi-Table Transaction"
    
    echo "  Running MySQL transaction..."
    if mysql_exec "START TRANSACTION;
        INSERT INTO users (username, email, status, balance) VALUES ('frank', 'frank@example.com', 'active', 500.00);
        SET @new_user_id = LAST_INSERT_ID();
        INSERT INTO orders (user_id, product_name, quantity, total_amount) 
            SELECT @new_user_id, 'Welcome Gift', 1, 0.00;
        UPDATE users SET balance = balance - 50.00 WHERE id = 1;
        COMMIT;"; then
        print_success "MySQL: Transaction completed"
    else
        print_error "MySQL: Transaction failed"
    fi
    
    echo "  Running PostgreSQL transaction..."
    if pg_exec "BEGIN;
        INSERT INTO users (username, email, status, balance) VALUES ('frank', 'frank@example.com', 'active', 500.00);
        INSERT INTO orders (user_id, product_name, quantity, total_amount) 
            VALUES (currval('users_id_seq'), 'Welcome Gift', 1, 0.00);
        UPDATE users SET balance = balance - 50.00 WHERE id = 1;
        COMMIT;"; then
        print_success "PostgreSQL: Transaction completed"
    else
        print_error "PostgreSQL: Transaction failed"
    fi
    sleep 2
    
    # Show summary
    print_header "Test Summary"
    
    echo "MySQL Statistics:"
    docker exec "$MYSQL_CONTAINER" mysql -u"$MYSQL_USER" -p"$MYSQL_PASS" "$MYSQL_DB" -t -e "
        SELECT 
            (SELECT COUNT(*) FROM users) as users_count,
            (SELECT COUNT(*) FROM orders) as orders_count,
            (SELECT COUNT(*) FROM events) as events_count;" 2>/dev/null || print_error "Could not get MySQL stats"
    
    echo ""
    echo "PostgreSQL Statistics:"
    docker exec "$PG_CONTAINER" psql -U "$PG_USER" -d "$PG_DB" -c "
        SELECT 
            (SELECT COUNT(*) FROM users) as users_count,
            (SELECT COUNT(*) FROM orders) as orders_count,
            (SELECT COUNT(*) FROM events) as events_count;" 2>/dev/null || print_error "Could not get PostgreSQL stats"

    
    print_header "Test Data Generation Complete!"
    
    echo ""
    echo "Check CDC outputs:"
    echo "  MySQL:      ./test_output/mysql/"
    echo "  PostgreSQL: ./test_output/postgres/"
    echo ""
    echo "Check logs:"
    echo "  MySQL:      mysql_cdc_test.log"
    echo "  PostgreSQL: pg_cdc_test.log"
    echo ""
    echo "View captured events:"
    echo "  cat test_output/mysql/*.json | jq ."
    echo "  cat test_output/postgres/*.json | jq ."
    echo ""
}

# Run main function
main "$@"