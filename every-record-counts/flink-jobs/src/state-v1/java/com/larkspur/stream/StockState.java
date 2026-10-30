package com.larkspur.stream;

/**
 * Version 1 of the detector's per-key state: the book stock of one store and SKU. A POJO, so Flink can evolve it
 * when a later version adds or removes fields (see src/state-v2).
 */
public class StockState {
    public int opening;         // units on the shelf this morning (the WMS file)
    public int level;           // opening minus every on-time unit sold so far, in event-time order
    public long outSince;       // event time the level reached zero; 0 = not out, or cancelled by a later sale
    public boolean alerted;     // STOCK_OUT already sent
    public boolean bookError;   // BOOK_ERROR already sent

    public StockState() {}
}
