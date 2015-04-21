package com.quantiply.rico.local;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.quantiply.rico.KeyValueStore;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by arrawatia on 3/19/15.
 */
public class LocalKeyValueStore implements KeyValueStore<String, Object> {

    private Map<String, Object> _internalStore;

    public LocalKeyValueStore() {
        this(null);
    }

    public LocalKeyValueStore(String file) {
        this._internalStore = new HashMap<>();
        if (file != null) {
            loadDataFromFile(file);
        }
    }

    private void loadDataFromFile(String fileName) {
        try {
            BufferedReader in = new BufferedReader(new FileReader(fileName));
            String str;
            while ((str = in.readLine()) != null) {
                TypeReference<HashMap<String, Object>> typeRef = new TypeReference<HashMap<String, Object>>() {
                };
                JsonFactory factory = new JsonFactory();
                ObjectMapper mapper = new ObjectMapper(factory);
                Map<String, Object> json = mapper.readValue(str, typeRef);
                put((String) json.get("key"), json.get("value"));
            }
            in.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Object get(String key) {
        return _internalStore.getOrDefault(key, null);
    }

    @Override
    public List<Object> get(List<String> keys) {
        List<Object> results = new ArrayList<>();
        for (String key : keys) {
            results.add(get(key));
        }
        return results;
    }

    @Override
    public void remove(String key) {
        _internalStore.remove(key);
    }

    @Override
    public void remove(List<String> keys) {
        for (String key : keys) {
            remove(key);
        }
    }

    @Override
    public void put(String key, Object value) {
        _internalStore.put(key, value);
    }

    @Override
    public void putAll(Map<String, Object> pairs) {
        for (Map.Entry<String, Object> entry : pairs.entrySet()) {
            put(entry.getKey(), entry.getValue());
        }
    }

}
