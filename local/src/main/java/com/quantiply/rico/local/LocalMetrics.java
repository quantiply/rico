package com.quantiply.rico.local;

import com.codahale.metrics.*;
import com.quantiply.rico.Metrics;

public class LocalMetrics implements Metrics {
    private final MetricRegistry _registry;
    private final String _group;

    public LocalMetrics(MetricRegistry registry, String group) {
        _registry = registry;
        _group = group;
    }

    @Override
    public <T extends Metric> T register(String name, T metric) {
        return _registry.register(MetricRegistry.name(_group, name), metric);
    }

    @Override
    public Histogram histogram(String name) {
        return _registry.histogram(MetricRegistry.name(_group, name));
    }

    @Override
    public Counter counter(String name) {
        return _registry.counter(MetricRegistry.name(_group, name));
    }

    @Override
    public Timer timer(String name) {
        return _registry.timer(MetricRegistry.name(_group, name));
    }

    @Override
    public Meter meter(String name) {
        return _registry.meter(MetricRegistry.name(_group, name));
    }

    @Override
    public Gauge gauge(String name, Gauge gauge) {
        return register(name, gauge);
    }
}
