package com.larkspur.stream;

import java.time.Duration;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Dedup, keyed by basket ID: the lines of the first copy of a basket to arrive pass, a re-send's are dropped
 * (Part 28's dedup by arrival, in Java). The state is one event ID per basket, forgotten after three days, the
 * re-send window Part 21 measured. State TTL counts processing time, not event time.
 */
public class FirstCopy extends KeyedProcessFunction<String, SaleLine, SaleLine> {

    private transient ValueState<String> firstEventId;

    @Override
    public void open(OpenContext openContext) {
        ValueStateDescriptor<String> d = new ValueStateDescriptor<>("first-event-id", String.class);
        d.enableTimeToLive(StateTtlConfig.newBuilder(Duration.ofDays(3)).build());
        firstEventId = getRuntimeContext().getState(d);
    }

    @Override
    public void processElement(SaleLine line, Context ctx, Collector<SaleLine> out) throws Exception {
        String first = firstEventId.value();
        if (first == null) {
            firstEventId.update(line.eventId);
            out.collect(line);
        } else if (first.equals(line.eventId)) {   // another line of the same copy
            out.collect(line);
        }
    }
}
