package com.quantiply.samza.util;

import com.codahale.metrics.Histogram;

public class EventStreamMetrics {
    public final Histogram lagFromOriginMs;
    public final Histogram lagFromPreviousMs;

    public EventStreamMetrics(StreamMetricRegistry registry) {
        lagFromOriginMs = registry.histogram("lag-from-origin-ms");
        lagFromPreviousMs = registry.histogram("lag-from-previous-step-ms");
    }
}
