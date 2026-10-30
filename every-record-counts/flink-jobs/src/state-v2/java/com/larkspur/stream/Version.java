package com.larkspur.stream;

import org.apache.flink.util.Collector;

/** Version 2: also warns when one unit is left, so a store can refill before the shelf is empty. */
final class Version {
    static final int NUMBER = 2;
    static final int LOW_STOCK_LEVEL = 1;

    private Version() {}

    /** Called after every sale: a LOW_STOCK alert the first time the level falls to one unit. */
    static void afterSale(String key, StockState s, int before, long ts, Collector<Alert> out) {
        if (s.lowSince == 0 && before > LOW_STOCK_LEVEL && s.level == LOW_STOCK_LEVEL) {
            s.lowSince = ts;
            out.collect(Alert.of(key, Alert.LOW_STOCK, ts, ts, s, NUMBER));
        }
    }
}
