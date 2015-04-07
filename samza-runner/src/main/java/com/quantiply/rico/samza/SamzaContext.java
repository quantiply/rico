package com.quantiply.rico.samza;

import com.codahale.metrics.*;
import com.quantiply.rico.Context;
import com.quantiply.rico.KeyValueStore;
import com.quantiply.rico.Metrics;
import org.apache.samza.task.TaskContext;

import java.util.HashMap;
import java.util.Map;

public class SamzaContext implements Context {

    private final TaskContext _internal;
    private final Map<String, KeyValueStore<String, Object>> _stores = new HashMap<>();
    private final Map<String, Metrics> _metrics = new HashMap<>();
    static final MetricRegistry _registry = new MetricRegistry();

    public SamzaContext(TaskContext ctx) {
        this._internal = ctx;
    }

    @Override
    public KeyValueStore getKeyValueStore(String name) {
        if(!_stores.containsKey(name)) {
            // This is ugly. Think of a better name for K,V store for Rico API.
            org.apache.samza.storage.kv.KeyValueStore<String, Object> s
                    = (org.apache.samza.storage.kv.KeyValueStore<String, Object>) _internal.getStore(name);
            _stores.put(name, new SamzaKeyValueStore(s));
        }
        return _stores.get(name);
    }

    @Override
    public Metrics metrics(String group) {
        if(!_metrics.containsKey(group)) {
            _metrics.put(group, new SamzaMetrics(_registry, _internal.getMetricsRegistry(), group));
        }
        return _metrics.get(group);
    }

}
