package com.quantiply.rico.samza;

import com.quantiply.rico.KeyValueStore;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SamzaKeyValueStore implements KeyValueStore<String, Object> {

    private org.apache.samza.storage.kv.KeyValueStore<String, Object> _internal;

    public SamzaKeyValueStore(org.apache.samza.storage.kv.KeyValueStore<String, Object> _internal) {
        this._internal = _internal;
    }

    @Override
    public Object get(String key) {
        return _internal.get(key);
    }

    @Override
    public List<Object> get(List<String> keys) {
        List<Object> results = new ArrayList<>();
        for(String key : keys){
            results.add(get(key));
        }
        return results;
    }

    @Override
    public void remove(String key) {
        _internal.delete(key);
    }

    @Override
    public void remove(List<String> keys) {
        for(String key: keys){
            remove(key);
        }
    }

    @Override
    public void put(String key, Object value) {
        _internal.put(key, value);
    }

    @Override
    public void putAll(Map<String, Object> pairs) {
        for(Map.Entry<String, Object> kv : pairs.entrySet()){
            put(kv.getKey(), kv.getValue());
        }
    }
}
