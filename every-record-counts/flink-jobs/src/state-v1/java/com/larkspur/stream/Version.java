package com.larkspur.stream;

import org.apache.flink.util.Collector;

/** Version 1: stock-out and book-error alerts only. */
final class Version {
    static final int NUMBER = 1;

    private Version() {}

    /** Called after every sale; version 1 adds nothing. */
    static void afterSale(String key, StockState s, int before, long ts, Collector<Alert> out) {}
}
