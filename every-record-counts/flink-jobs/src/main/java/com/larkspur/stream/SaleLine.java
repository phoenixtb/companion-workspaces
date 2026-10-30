package com.larkspur.stream;

/**
 * One line of a till basket: what left the shelf, when (event time) and when the till uploaded it.
 * A Flink POJO (public class, public no-argument constructor, public fields), so Flink serialises it with its own
 * POJO serializer rather than Kryo, and its schema can evolve in state.
 */
public class SaleLine {
    public String eventId;       // unique per send: a re-sent basket gets a new one
    public String basketId;      // the business key: re-sends repeat it
    public String storeId;
    public String sku;
    public int qty;
    public long eventTs;         // epoch ms, the till's clock (UTC)
    public long uploadedAt;      // epoch ms, when the till uploaded the basket
    public String businessDate;  // the store's local date of eventTs (Part 5)
    public long watermark;       // set only on late lines: the detector's watermark when the line arrived

    public SaleLine() {}

    /** The detector's key: one stock level per store and SKU. */
    public String storeSku() {
        return storeId + "|" + sku;
    }
}
