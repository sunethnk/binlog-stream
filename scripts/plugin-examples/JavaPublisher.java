// TestPublisher.java
import java.util.Map;
import java.util.HashMap;

public class JavaPublisher {
    private Map<String, Integer> stats;
    
    public JavaPublisher() {
        this.stats = new HashMap<>();
        this.stats.put("total", 0);
    }
    
    // Main event handler (called for each CDC event)
    public int onEvent(Map<String, String> event) {
        int total = stats.get("total") + 1;
        stats.put("total", total);
        
        // Track by table
        String tableKey = event.getOrDefault("db", "unknown") + "." + 
                         event.getOrDefault("table", "unknown");
        stats.put(tableKey, stats.getOrDefault(tableKey, 0) + 1);
        
        // Print event
        System.out.println("=== Event #" + total + " ===");
        System.out.println("Transaction: " + event.getOrDefault("txn", "N/A"));
        System.out.println("Database: " + event.getOrDefault("db", "N/A"));
        System.out.println("Table: " + event.getOrDefault("table", "N/A"));
        System.out.println("JSON: " + event.getOrDefault("json", "N/A"));
        System.out.println("==============================");
        
        return 0; // Success
    }
    
    // Optional: Called on init
    public int onInit(Map<String, String> config) {
        System.out.println("[Java] Initializing test publisher");
        System.out.println("[Java] Configuration:");
        for (Map.Entry<String, String> entry : config.entrySet()) {
            System.out.println("  " + entry.getKey() + " = " + entry.getValue());
        }
        return 0;
    }
    
    // Optional: Called on start
    public int onStart() {
        System.out.println("[Java] Starting test publisher");
        stats.clear();
        stats.put("total", 0);
        return 0;
    }
    
    // Optional: Called on stop
    public int onStop() {
        System.out.println("[Java] Stopping test publisher");
        System.out.println("[Java] Total events: " + stats.get("total"));
        System.out.println("[Java] Events by table:");
        for (Map.Entry<String, Integer> entry : stats.entrySet()) {
            if (!entry.getKey().equals("total")) {
                System.out.println("  " + entry.getKey() + ": " + entry.getValue());
            }
        }
        return 0;
    }
    
    // Optional: Called on cleanup
    public void onCleanup() {
        System.out.println("[Java] Cleanup complete");
    }
    
    // Optional: Called for health check
    public int onHealth() {
        System.out.println("[Java] Health check: " + stats.get("total") + " events");
        return 0; // Healthy
    }
}
