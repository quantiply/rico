package com.quantiply.rico.core;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.quantiply.rico.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Map;

/**
 * Created by arrawatia on 1/29/15.
 */
public class Configurator {

    private final static Logger LOG = LoggerFactory.getLogger(Configurator.class);

    Map<String, Object> _cfg;
    String _cfgPath;

    public Configurator(String cfgPath) {
        _cfgPath = cfgPath;
        _cfg = readConfig(_cfgPath);
    }

    public Configurator(String defaultPath, String cfgPath) {
        _cfgPath = cfgPath;
        Map<String, Object> defaults = readConfig(defaultPath);
        Map<String, Object> cfg = readConfig(_cfgPath);
        _cfg = deepMerge(defaults, cfg);
//        MapUtils.debugPrint(System.out, null, _cfg);
    }

    private static Map<String, Object> readConfig(String fileName) {
        ObjectMapper ymapper = new ObjectMapper(new YAMLFactory());

        Map<String, Object> result = null;
        try {
            result = ymapper.readValue(new File(fileName), new TypeReference<Map<String, Object>>() {});
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return result;
    }


    public Configuration get(String name) {
        return new Configuration(getAsMap(name));
    }

    public Map getAsMap(String name) {
        if (!_cfg.containsKey(name)) {
            throw new RuntimeException(name + "not found in config " + _cfgPath);
        }
        return (Map) _cfg.get(name);
    }

    public Map<String, String> getAsStringMap(String name) {
        if (!_cfg.containsKey(name)) {
            throw new RuntimeException(name + "not found in config " + _cfgPath);
        }
        return (Map<String, String>) _cfg.get(name);
    }

    public Map deepMerge(Map original, Map newMap) {
        for (Object key : newMap.keySet()) {
            if (newMap.get(key) instanceof Map && original.get(key) instanceof Map) {
                Map originalChild = (Map) original.get(key);
                Map newChild = (Map) newMap.get(key);
                original.put(key, deepMerge(originalChild, newChild));
            } else {
                original.put(key, newMap.get(key));
            }
        }
        return original;
    }
}
