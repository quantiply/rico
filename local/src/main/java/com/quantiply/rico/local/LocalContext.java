package com.quantiply.rico.local;

import com.codahale.metrics.*;
import com.quantiply.rico.Configuration;
import com.quantiply.rico.Context;
import com.quantiply.rico.KeyValueStore;
import com.quantiply.rico.Metrics;

import java.io.File;
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
        String stores = cfg.getString("stores.kv");
        if(stores != null) {
            for(String store: stores.split(",") ) {
                String initFile = cfg.getString("path.stores") + store;
                addKeyValueStore(store, new File(initFile).exists() ? initFile : null);
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
