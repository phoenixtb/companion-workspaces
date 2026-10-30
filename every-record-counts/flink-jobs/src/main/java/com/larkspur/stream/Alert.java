package com.larkspur.stream;

/** What the detector emits. One alert of each kind per store, SKU and day at most. */
public class Alert {
    public static final String STOCK_OUT = "STOCK_OUT";    // book stock at zero, and no sale for the confirm window
    public static final String BOOK_ERROR = "BOOK_ERROR";  // a till sold what the books say isn't there
    public static final String LOW_STOCK = "LOW_STOCK";    // version 2: one unit left

    public String storeId;
    public String sku;
    public String kind;
    public long eventTs;       // epoch ms: when the level reached the condition (the sale that did it)
    public long detectedTs;    // epoch ms, event time: when the rule decided (STOCK_OUT: eventTs + confirm window)
    public int level;          // book stock after that sale
    public int opening;        // the morning's opening stock
    public int jobVersion;     // which version of the job emitted it (Step 5's upgrade)

    public Alert() {}

    static Alert of(String key, String kind, long eventTs, long detectedTs, StockState s, int jobVersion) {
        Alert a = new Alert();
        String[] storeSku = key.split("\\|");
        a.storeId = storeSku[0];
        a.sku = storeSku[1];
        a.kind = kind;
        a.eventTs = eventTs;
        a.detectedTs = detectedTs;
        a.level = s.level;
        a.opening = s.opening;
        a.jobVersion = jobVersion;
        return a;
    }

    /** A small JSON document for the Kafka alert topics (the store's pager would read these). */
    String toJson() {
        return String.format(
                "{\"store_id\":\"%s\",\"sku\":\"%s\",\"kind\":\"%s\",\"event_ts\":%d,\"detected_ts\":%d,\"level\":%d}",
                storeId, sku, kind, eventTs, detectedTs, level);
    }
}
