package com.larkspur.stream;

import java.time.Duration;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;

/**
 * Bounded out-of-orderness, like Flink's own generator, but emitting a watermark after every event instead of every
 * 200 ms. A replay reads hours of event time per second; a periodic watermark would trail it by whatever was read in
 * the last interval, and which lines count as late would depend on machine speed (Part 26's Gotcha).
 */
public class PerEventWatermarks implements WatermarkGenerator<SaleLine> {

    private final long boundMs;
    private long maxTs = Long.MIN_VALUE;

    public PerEventWatermarks(Duration bound) {
        this.boundMs = bound.toMillis();
    }

    @Override
    public void onEvent(SaleLine line, long eventTimestamp, WatermarkOutput output) {
        if (eventTimestamp > maxTs) {
            maxTs = eventTimestamp;
            output.emitWatermark(new Watermark(maxTs - boundMs - 1));   // same arithmetic as Flink's bounded generator
        }
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput output) {}
}
