package com.larkspur.stream;

/**
 * Version 2 of the detector's per-key state: one field more than version 1. Flink's POJO serializer restores a
 * version 1 savepoint into this class and sets the new field to its Java default (0).
 */
public class StockState {
    public int opening;         // units on the shelf this morning (the WMS file)
    public int level;           // opening minus every on-time unit sold so far, in event-time order
    public long outSince;       // event time the level reached zero; 0 = not out, or cancelled by a later sale
    public boolean alerted;     // STOCK_OUT already sent
    public boolean bookError;   // BOOK_ERROR already sent
    public long lowSince;       // new in version 2: event time the level fell to LOW_STOCK_LEVEL; 0 = not yet

    public StockState() {}
}
