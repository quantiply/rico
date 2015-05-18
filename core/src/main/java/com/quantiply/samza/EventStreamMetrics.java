package com.quantiply.samza;

import com.codahale.metrics.Histogram;

public class EventStreamMetrics {
    public final Histogram lagFromOriginMs;
    public final Histogram lagFromPreviousMs;

    public EventStreamMetrics(MetricAdaptor registry) {
        lagFromOriginMs = registry.histogram("lag-from-origin-ms");
        lagFromPreviousMs = registry.histogram("lag-from-previous-step-ms");
    }
}
