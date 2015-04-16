package com.quantiply.samza.util;

import com.codahale.metrics.Histogram;

public class EventStreamMetrics {
    public final Histogram lagFromEventMs;

    public EventStreamMetrics(StreamMetricRegistry registry) {
        lagFromEventMs = registry.histogram("lag-from-origin-ms");
    }
}
