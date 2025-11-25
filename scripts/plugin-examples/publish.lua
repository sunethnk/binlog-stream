-- test_publisher.lua
-- Simple test publisher for lua_publisher.so

local stats = {
    total = 0,
    by_table = {}
}

-- Main event handler (called for each CDC event)
function on_event(event)
    stats.total = stats.total + 1
    
    -- Track events by table
    local table_key = string.format("%s.%s", event.db or "unknown", event.table or "unknown")
    stats.by_table[table_key] = (stats.by_table[table_key] or 0) + 1
    
    -- Print event details
    print(string.format("=== Event #%d ===", stats.total))
    print(string.format("Transaction: %s", event.txn or "N/A"))
    print(string.format("Database: %s", event.db or "N/A"))
    print(string.format("Table: %s", event.table or "N/A"))
    print(string.format("JSON: %s", event.json or "N/A"))
    print("==================")
    
    return 0  -- Success
end

-- Optional: Called on init
function on_init(config)
    print("[Lua] Initializing test publisher")
    print("[Lua] Configuration received:")
    for k, v in pairs(config) do
        print(string.format("  %s = %s", k, v))
    end
    return 0
end

-- Optional: Called on start
function on_start()
    print("[Lua] Starting test publisher")
    stats.total = 0
    stats.by_table = {}
    return 0
end

-- Optional: Called on stop
function on_stop()
    print("[Lua] Stopping test publisher")
    print(string.format("[Lua] Total events processed: %d", stats.total))
    print("[Lua] Events by table:")
    for table_name, count in pairs(stats.by_table) do
        print(string.format("  %s: %d", table_name, count))
    end
    return 0
end

-- Optional: Called on cleanup
function on_cleanup()
    print("[Lua] Cleanup complete")
end

-- Optional: Called for health check
function on_health()
    print(string.format("[Lua] Health check: %d events processed", stats.total))
    return 0  -- Healthy
end