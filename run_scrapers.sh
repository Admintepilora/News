#!/bin/bash
# Script to run all news scrapers as separate processes with database options

# Set working directory to the script location
cd "$(dirname "$0")"

# Default database settings
USE_MONGODB=true
USE_CLICKHOUSE=true
CHECK_ACROSS_DBS=true

# Function to print colored output
print_colored() {
    COLOR='\033[0;36m' # Cyan
    NC='\033[0m' # No Color
    echo -e "${COLOR}[$(date '+%Y-%m-%d %H:%M:%S')] $1${NC}"
}

# Function to run a scraper in the background
run_scraper() {
    SCRAPER=$1
    LOG_FILE="${SCRAPER%.*}.log"
    DB_PARAMS=""
    
    # Build database parameters based on current settings
    if [ "$USE_MONGODB" = false ]; then
        DB_PARAMS="$DB_PARAMS --no-mongodb"
    fi
    
    if [ "$USE_CLICKHOUSE" = false ]; then
        DB_PARAMS="$DB_PARAMS --no-clickhouse"
    fi
    
    # The --check-across-dbs parameter is not currently supported in the scripts
    # We'll modify the scripts directly to enable this functionality
    
    print_colored "Starting $SCRAPER with options: $DB_PARAMS (logs in $LOG_FILE)"
    # Run in background and redirect output to log file
    python3 "$SCRAPER" $DB_PARAMS > "$LOG_FILE" 2>&1 &
    
    # Store the PID of the process
    echo $! > "${SCRAPER%.*}.pid"
    print_colored "$SCRAPER started with PID $(cat ${SCRAPER%.*}.pid)"
}

# Function to check if a scraper is running
is_running() {
    PID_FILE="${1%.*}.pid"
    if [ -f "$PID_FILE" ]; then
        PID=$(cat "$PID_FILE")
        if ps -p "$PID" > /dev/null; then
            return 0 # Running
        fi
    fi
    return 1 # Not running
}

# Function to stop a scraper
stop_scraper() {
    SCRAPER=$1
    PID_FILE="${SCRAPER%.*}.pid"
    
    if [ -f "$PID_FILE" ]; then
        PID=$(cat "$PID_FILE")
        print_colored "Stopping $SCRAPER (PID: $PID)"
        kill "$PID" 2>/dev/null || print_colored "$SCRAPER was not running"
        rm -f "$PID_FILE"
    else
        print_colored "$SCRAPER is not running"
    fi
}

# Process database options
shift_count=0
while [[ "$1" == --* ]]; do
    case "$1" in
        --no-mongodb)
            USE_MONGODB=false
            shift
            ((shift_count++))
            ;;
        --no-clickhouse)
            USE_CLICKHOUSE=false
            shift
            ((shift_count++))
            ;;
        --no-check-across)
            CHECK_ACROSS_DBS=false
            shift
            ((shift_count++))
            ;;
        --clickhouse-only)
            USE_MONGODB=false
            USE_CLICKHOUSE=true
            shift
            ((shift_count++))
            ;;
        --mongodb-only)
            USE_MONGODB=true
            USE_CLICKHOUSE=false
            shift
            ((shift_count++))
            ;;
        *)
            break
            ;;
    esac
done

# Restore the first argument for the main case statement
if [ $shift_count -gt 0 ]; then
    set -- "$1" "${@:2}"
fi

# Print current database settings
print_db_settings() {
    print_colored "Database settings:"
    print_colored "  MongoDB: $([ "$USE_MONGODB" = true ] && echo 'enabled' || echo 'disabled')"
    print_colored "  ClickHouse: $([ "$USE_CLICKHOUSE" = true ] && echo 'enabled' || echo 'disabled')"
    print_colored "  Cross-DB Check: $([ "$CHECK_ACROSS_DBS" = true ] && echo 'enabled' || echo 'disabled')"
}

# Command line options
case "$1" in
    start)
        print_colored "Starting all news scrapers..."
        print_db_settings
        
        # Start each scraper
        run_scraper "DuckDuckGoApiNews.py"
        sleep 2 # Add a delay to avoid starting all at once
        
        run_scraper "GNewsApiNews.py"
        sleep 2
        
        run_scraper "WebSitesNews.py"
        
        print_colored "All scrapers started"
        ;;
        
    stop)
        print_colored "Stopping all news scrapers..."
        
        # Stop each scraper
        stop_scraper "DuckDuckGoApiNews.py"
        stop_scraper "GNewsApiNews.py"
        stop_scraper "WebSitesNews.py"
        
        print_colored "All scrapers stopped"
        ;;
        
    restart)
        # Stop then start - preserve additional args for start
        "$0" stop
        sleep 2
        
        # Start with all the remaining arguments
        "$0" "$@"
        ;;
        
    status)
        print_colored "Checking scraper status..."
        
        # Check status of each scraper
        if is_running "DuckDuckGoApiNews.py"; then
            print_colored "DuckDuckGoApiNews.py is running (PID: $(cat DuckDuckGoApiNews.pid))"
        else
            print_colored "DuckDuckGoApiNews.py is not running"
        fi
        
        if is_running "GNewsApiNews.py"; then
            print_colored "GNewsApiNews.py is running (PID: $(cat GNewsApiNews.pid))"
        else
            print_colored "GNewsApiNews.py is not running"
        fi
        
        if is_running "WebSitesNews.py"; then
            print_colored "WebSitesNews.py is running (PID: $(cat WebSitesNews.pid))"
        else
            print_colored "WebSitesNews.py is not running"
        fi
        ;;
        
    test)
        print_colored "Running all scrapers in test mode..."
        print_db_settings
        
        # Set test mode (temporary)
        sed -i 's/test_mode = False/test_mode = True/g' DuckDuckGoApiNews.py
        sed -i 's/test_mode = False/test_mode = True/g' GNewsApiNews.py
        sed -i 's/test_mode = False/test_mode = True/g' WebSitesNews.py
        
        # Build database parameters
        DB_PARAMS=""
        if [ "$USE_MONGODB" = false ]; then
            DB_PARAMS="$DB_PARAMS --no-mongodb"
        fi
        
        if [ "$USE_CLICKHOUSE" = false ]; then
            DB_PARAMS="$DB_PARAMS --no-clickhouse"
        fi
        
        # The --check-across-dbs parameter is handled directly in the scripts
        
        # Run each scraper directly (not in background)
        print_colored "Testing DuckDuckGoApiNews.py with options: $DB_PARAMS"
        python3 DuckDuckGoApiNews.py $DB_PARAMS
        
        print_colored "Testing GNewsApiNews.py with options: $DB_PARAMS"
        python3 GNewsApiNews.py $DB_PARAMS
        
        print_colored "Testing WebSitesNews.py with options: $DB_PARAMS"
        python3 WebSitesNews.py $DB_PARAMS
        
        # Reset test mode
        sed -i 's/test_mode = True/test_mode = False/g' DuckDuckGoApiNews.py
        sed -i 's/test_mode = True/test_mode = False/g' GNewsApiNews.py
        sed -i 's/test_mode = True/test_mode = False/g' WebSitesNews.py
        
        print_colored "All tests completed"
        ;;
        
    *)
        echo "Usage: $0 [database options] {start|stop|restart|status|test}"
        echo
        echo "Database Options:"
        echo "  --no-mongodb      - Disable MongoDB storage"
        echo "  --no-clickhouse   - Disable ClickHouse storage"
        echo "  --no-check-across - Disable cross-database duplicate checking"
        echo "  --clickhouse-only - Enable only ClickHouse storage (shorthand for --no-mongodb)"
        echo "  --mongodb-only    - Enable only MongoDB storage (shorthand for --no-clickhouse)"
        echo
        echo "Commands:"
        echo "  start   - Start all scrapers"
        echo "  stop    - Stop all scrapers"
        echo "  restart - Restart all scrapers"
        echo "  status  - Show status of all scrapers"
        echo "  test    - Run all scrapers in test mode"
        exit 1
        ;;
esac

exit 0