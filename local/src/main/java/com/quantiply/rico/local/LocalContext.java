package com.quantiply.rico.local;

import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.MetricRegistry;
import com.quantiply.rico.Configuration;
import com.quantiply.rico.Context;
import com.quantiply.rico.KeyValueStore;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by arrawatia on 3/19/15.
 */
public class LocalContext implements Context {
    static final MetricRegistry _registry = new MetricRegistry();

    Map<String, KeyValueStore> _stores;

    public LocalContext(Configuration cfg) {
        _stores = new HashMap<>();
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

    public MetricRegistry getMetricsRegistry() {
        return _registry;
    }
}
