package com.quantiply.samza;

import com.codahale.metrics.*;
import com.quantiply.rico.Metrics;
import com.quantiply.samza.MetricAdaptor;

public class StreamMetricRegistry implements Metrics {
    private final String namePrefix;
    private final MetricAdaptor metricAdaptor;

    public StreamMetricRegistry(String namePrefix, MetricAdaptor metricAdaptor) {
        this.namePrefix = namePrefix;
        this.metricAdaptor = metricAdaptor;
    }

    @Override
    public <T extends Metric> T register(String name, T metric) {
        return metricAdaptor.register(getStreamMetricName(name), metric);
    }

    @Override
    public Histogram histogram(String name) {
        return metricAdaptor.histogram(getStreamMetricName(name));
    }

    @Override
    public Counter counter(String name) {
        return metricAdaptor.counter(getStreamMetricName(name));
    }

    @Override
    public Timer timer(String name) {
        return metricAdaptor.timer(getStreamMetricName(name));
    }

    @Override
    public Meter meter(String name) {
        return metricAdaptor.meter(getStreamMetricName(name));
    }

    @Override
    public Gauge gauge(String name, Gauge gauge) {
        return metricAdaptor.gauge(getStreamMetricName(name), gauge);
    }

    private String getStreamMetricName(String name) {
        return namePrefix + name;
    }
}
