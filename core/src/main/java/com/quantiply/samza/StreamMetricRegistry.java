package com.quantiply.samza;

import com.codahale.metrics.*;

public class StreamMetricRegistry {

    public static String sanitizeStreamNameForMetrics(String streamName) {
        return streamName.replaceAll("\\.", "_");
    }

    private final String namePrefix;
    private final MetricAdaptor metricAdaptor;

    public StreamMetricRegistry(String namePrefix, MetricAdaptor metricAdaptor) {
        this.namePrefix = namePrefix;
        this.metricAdaptor = metricAdaptor;
    }

    public <T extends Metric> T register(String name, T metric) {
        return metricAdaptor.register(getStreamMetricName(name), metric);
    }

    public Histogram histogram(String name) {
        return metricAdaptor.histogram(getStreamMetricName(name));
    }

    public Counter counter(String name) {
        return metricAdaptor.counter(getStreamMetricName(name));
    }

    public Timer timer(String name) {
        return metricAdaptor.timer(getStreamMetricName(name));
    }

    public Meter meter(String name) {
        return metricAdaptor.meter(getStreamMetricName(name));
    }

    public Gauge gauge(String name, Gauge gauge) {
        return metricAdaptor.gauge(getStreamMetricName(name), gauge);
    }

    private String getStreamMetricName(String name) {
        return namePrefix + name;
    }
}
