#!/bin/bash
# Quick Setup Script for CDC Testing
# Sets up Docker containers and runs CDC streamers

set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

print_header() {
    echo ""
    echo -e "${BLUE}=========================================${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}=========================================${NC}"
    echo ""
}

print_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

print_info() {
    echo -e "${YELLOW}→ $1${NC}"
}

print_error() {
    echo -e "${RED}✗ $1${NC}"
}

# Check prerequisites
check_prerequisites() {
    print_header "Checking Prerequisites"
    
    if ! command -v docker &> /dev/null; then
        print_error "Docker not found. Please install Docker."
        exit 1
    fi
    print_success "Docker found"
    
    if ! command -v docker-compose &> /dev/null && ! docker compose version &> /dev/null; then
        print_error "Docker Compose not found. Please install Docker Compose."
        exit 1
    fi
    print_success "Docker Compose found"
    
    if [ ! -f "Makefile" ]; then
        print_error "Makefile not found. Please run from project root."
        exit 1
    fi
    print_success "Project structure OK"
}

# Build CDC streamers
build_streamers() {
    print_header "Building CDC Streamers"
    
    print_info "Building MySQL CDC streamer..."
    make mysql
    print_success "MySQL CDC streamer built"
    
    print_info "Building PostgreSQL CDC streamer..."
    make postgres
    print_success "PostgreSQL CDC streamer built"
    
    print_info "Building file publisher plugin..."
    mkdir -p build/lib
    gcc -O2 -Wall -fPIC -Isrc/include -shared \
        -o build/lib/file_publisher.so \
        src/plugins/file_publisher.c 2>/dev/null || echo "Note: Using existing plugin"
    print_success "Publisher plugin ready"
}

# Setup Docker environment
setup_docker() {
    print_header "Setting Up Docker Environment"
    
    # Create directories
    mkdir -p docker/mysql-init docker/postgres-init test_output/mysql test_output/postgres
    
    # Make init scripts executable
    chmod +x docker/mysql-init/*.sh 2>/dev/null || true
    chmod +x docker/postgres-init/*.sh 2>/dev/null || true
    chmod +x docker/test-data.sh 2>/dev/null || true
    
    print_info "Starting Docker containers..."
    docker-compose down -v 2>/dev/null || true
    docker-compose up -d
    
    print_info "Waiting for databases to be ready..."
    sleep 10
    
    # Wait for MySQL
    print_info "Waiting for MySQL to be healthy..."
    timeout 60 bash -c 'until docker exec cdc-mysql mysqladmin ping -h localhost -uroot -prootpass &>/dev/null; do sleep 2; done'
    print_success "MySQL is ready"
    
    # Wait for PostgreSQL
    print_info "Waiting for PostgreSQL to be healthy..."
    timeout 60 bash -c 'until docker exec cdc-postgres pg_isready -U postgres &>/dev/null; do sleep 2; done'
    print_success "PostgreSQL is ready"
    
    sleep 5  # Extra time for initialization scripts
}

# Verify database setup
verify_databases() {
    print_header "Verifying Database Setup"
    
    print_info "Checking MySQL tables..."
    docker exec cdc-mysql mysql -uroot -prootpass testdb -e "SHOW TABLES;" | grep -q users
    print_success "MySQL tables created"
    
    print_info "Checking MySQL binlog..."
    docker exec cdc-mysql mysql -uroot -prootpass -e "SHOW MASTER STATUS\G" | grep -q "File:"
    print_success "MySQL binlog enabled"
    
    print_info "Checking PostgreSQL tables..."
    docker exec cdc-postgres psql -U postgres testdb -c "\dt" | grep -q users
    print_success "PostgreSQL tables created"
    
    print_info "Checking PostgreSQL replication slot..."
    docker exec cdc-postgres psql -U postgres testdb -c "SELECT * FROM pg_replication_slots WHERE slot_name='cdc_slot';" | grep -q cdc_slot
    print_success "PostgreSQL replication slot created"
}

# Start CDC streamers
start_streamers() {
    print_header "Starting CDC Streamers"
    
    # Kill any existing streamers
    pkill -f binlog_stream || true
    pkill -f pg_stream || true
    sleep 2
    
    print_info "Starting MySQL CDC streamer..."
    nohup ./build/bin/binlog_stream docker/config-mysql-test.json > mysql_streamer.out 2>&1 &
    MYSQL_PID=$!
    print_success "MySQL CDC streamer started (PID: $MYSQL_PID)"
    
    sleep 3
    
    print_info "Starting PostgreSQL CDC streamer..."
    nohup ./build/bin/pg_stream docker/config-postgres-test.json > pg_streamer.out 2>&1 &
    PG_PID=$!
    print_success "PostgreSQL CDC streamer started (PID: $PG_PID)"
    
    sleep 5
    
    # Check if streamers are running
    if ps -p $MYSQL_PID > /dev/null; then
        print_success "MySQL CDC streamer is running"
    else
        print_error "MySQL CDC streamer failed to start"
        cat mysql_streamer.out
    fi
    
    if ps -p $PG_PID > /dev/null; then
        print_success "PostgreSQL CDC streamer is running"
    else
        print_error "PostgreSQL CDC streamer failed to start"
        cat pg_streamer.out
    fi
}

# Run tests
run_tests() {
    print_header "Running CDC Tests"
    
    print_info "Generating test data..."
    chmod +x docker/test-data.sh
    ./docker/test-data.sh
    
    sleep 5
    
    print_info "Checking CDC outputs..."
    
    if [ -f "test_output/mysql/mysql_cdc_testdb.users.json" ] || \
       [ -n "$(find test_output/mysql -name '*.json' 2>/dev/null)" ]; then
        print_success "MySQL CDC captured events"
        echo -e "   Files: $(find test_output/mysql -name '*.json' | wc -l) JSON files"
    else
        print_error "No MySQL CDC output found"
    fi
    
    if [ -f "test_output/postgres/pg_cdc_public.users.json" ] || \
       [ -n "$(find test_output/postgres -name '*.json' 2>/dev/null)" ]; then
        print_success "PostgreSQL CDC captured events"
        echo -e "   Files: $(find test_output/postgres -name '*.json' | wc -l) JSON files"
    else
        print_error "No PostgreSQL CDC output found"
    fi
}

# Show results
show_results() {
    print_header "Test Results"
    
    echo ""
    echo "Database Status:"
    echo "  MySQL:      Running on localhost:3306"
    echo "  PostgreSQL: Running on localhost:5432"
    echo ""
    
    echo "CDC Streamers:"
    if pgrep -f binlog_stream > /dev/null; then
        echo -e "  MySQL CDC:      ${GREEN}Running${NC} (PID: $(pgrep -f binlog_stream))"
    else
        echo -e "  MySQL CDC:      ${RED}Stopped${NC}"
    fi
    
    if pgrep -f pg_stream > /dev/null; then
        echo -e "  PostgreSQL CDC: ${GREEN}Running${NC} (PID: $(pgrep -f pg_stream))"
    else
        echo -e "  PostgreSQL CDC: ${RED}Stopped${NC}"
    fi
    echo ""
    
    echo "Output Directories:"
    echo "  MySQL:      test_output/mysql/"
    if [ -d "test_output/mysql" ]; then
        echo "              $(find test_output/mysql -name '*.json' 2>/dev/null | wc -l) files"
    fi
    echo "  PostgreSQL: test_output/postgres/"
    if [ -d "test_output/postgres" ]; then
        echo "              $(find test_output/postgres -name '*.json' 2>/dev/null | wc -l) files"
    fi
    echo ""
    
    echo "Log Files:"
    echo "  MySQL CDC:      mysql_cdc_test.log"
    echo "  PostgreSQL CDC: pg_cdc_test.log"
    echo "  MySQL Output:   mysql_streamer.out"
    echo "  PG Output:      pg_streamer.out"
    echo ""
    
    echo "Useful Commands:"
    echo "  # View CDC events"
    echo "  cat test_output/mysql/*.json | jq ."
    echo "  cat test_output/postgres/*.json | jq ."
    echo ""
    echo "  # Tail logs"
    echo "  tail -f mysql_cdc_test.log"
    echo "  tail -f pg_cdc_test.log"
    echo ""
    echo "  # Connect to databases"
    echo "  docker exec -it cdc-mysql mysql -uroot -prootpass testdb"
    echo "  docker exec -it cdc-postgres psql -U postgres testdb"
    echo ""
    echo "  # Generate more test data"
    echo "  ./docker/test-data.sh"
    echo ""
    echo "  # Stop everything"
    echo "  ./quick-test.sh stop"
    echo ""
}

# Stop everything
stop_all() {
    print_header "Stopping CDC Test Environment"
    
    print_info "Stopping CDC streamers..."
    pkill -f binlog_stream || true
    pkill -f pg_stream || true
    print_success "CDC streamers stopped"
    
    print_info "Stopping Docker containers..."
    docker-compose down
    print_success "Docker containers stopped"
}

# Cleanup
cleanup_all() {
    print_header "Cleaning Up Test Environment"
    
    stop_all
    
    print_info "Removing test data..."
    rm -rf test_output/*
    rm -f mysql_cdc_test.log pg_cdc_test.log
    rm -f mysql_streamer.out pg_streamer.out
    rm -f mysql_checkpoint.dat pg_checkpoint.dat
    
    docker-compose down -v
    print_success "Cleanup complete"
}

# Main execution
main() {
    case "${1:-start}" in
        start)
            check_prerequisites
            build_streamers
            setup_docker
            verify_databases
            start_streamers
            sleep 2
            run_tests
            show_results
            ;;
        stop)
            stop_all
            ;;
        clean)
            cleanup_all
            ;;
        test)
            run_tests
            ;;
        status)
            show_results
            ;;
        *)
            echo "Usage: $0 {start|stop|clean|test|status}"
            echo ""
            echo "Commands:"
            echo "  start   - Start everything and run tests (default)"
            echo "  stop    - Stop CDC streamers and Docker containers"
            echo "  clean   - Stop and remove all test data"
            echo "  test    - Run test data generation again"
            echo "  status  - Show current status"
            exit 1
            ;;
    esac
}

main "$@"
