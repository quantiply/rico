package com.quantiply.rico.local;

import com.codahale.metrics.*;
import com.quantiply.rico.Configuration;
import com.quantiply.rico.Context;
import com.quantiply.rico.KeyValueStore;
import com.quantiply.rico.Metrics;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by arrawatia on 3/19/15.
 */
public class LocalContext implements Context {
    static final MetricRegistry _registry = new MetricRegistry();

    private final Map<String, KeyValueStore> _stores = new HashMap<>();
    private final Map<String, Metrics> _metrics = new HashMap<>();

    public LocalContext(Configuration cfg) {
        Map<String, Object> storeCfg = (Map<String, Object>) cfg.get("store");

        // Oh ! How I hate working with collections in Java.
        if(storeCfg != null) {
            for (Map.Entry<String, Object> entry : storeCfg.entrySet()) {
                String name = entry.getKey();
                Map<String, String> val = (Map<String, String>) entry.getValue();
//                System.out.println("Data store - name: "+ name);
                addKeyValueStore(name, val.getOrDefault("data", null));
            }
        }
        final JmxReporter reporter = JmxReporter.forRegistry(_registry).build();
        reporter.start();
    }

    public void addKeyValueStore(String name, String initialDataFile){
        _stores.put(name, new LocalKeyValueStore(initialDataFile));
    }

    @Override
    public KeyValueStore getKeyValueStore(String name) {
        return _stores.get(name);
    }

    @Override
    public Metrics metrics(String group) {
        if(!_metrics.containsKey(group)) {
            _metrics.put(group, new LocalMetrics(_registry, group));
        }
        return _metrics.get(group);
    }

}
