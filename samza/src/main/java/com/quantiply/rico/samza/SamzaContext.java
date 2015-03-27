package com.quantiply.rico.samza;

import com.codahale.metrics.*;
import com.quantiply.rico.Context;
import com.quantiply.rico.KeyValueStore;
import org.apache.samza.task.TaskContext;

import java.util.HashMap;
import java.util.Map;

public class SamzaContext implements Context{

    private TaskContext _internal;
    private Map<String, KeyValueStore<String, Object>> stores  = new HashMap<>();
    static final MetricRegistry _registry = new MetricRegistry();


    public SamzaContext(TaskContext ctx) {
        this._internal = ctx;
    }

    @Override
    public KeyValueStore getKeyValueStore(String name) {
        if(!stores.containsKey(name)) {
            // This is ugly. Think of a better name for K,V store for Rico API.
            org.apache.samza.storage.kv.KeyValueStore<String, Object> s
                    = (org.apache.samza.storage.kv.KeyValueStore<String, Object>) _internal.getStore(name);
            stores.put(name, new SamzaKeyValueStore(s));
        }
        return stores.get(name);
    }

    @Override
    public Histogram histogram(String name) {
        Histogram h = _registry.histogram(name);
        _internal.getMetricsRegistry().newGauge(name, new MapGauge(name, h));
        return h;
    }

    @Override
    public Counter counter(String name) {
        Counter h = _registry.counter(name);
        _internal.getMetricsRegistry().newGauge(name, new MapGauge(name, h));
        return h;
    }

    @Override
    public Timer timer(String name) {
        Timer h = _registry.timer(name);
        _internal.getMetricsRegistry().newGauge(name, new MapGauge(name, h));
        return h;
    }

    @Override
    public Meter meter(String name) {
        Meter h = _registry.meter(name);
        _internal.getMetricsRegistry().newGauge(name, new MapGauge(name, h));
        return h;
    }

    @Override
    public void gauge(String name, Gauge g) {
        _internal.getMetricsRegistry().newGauge(name, new MapGauge(name, g));
    }
}
