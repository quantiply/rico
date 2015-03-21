package com.quantiply.rico.api;

import java.util.Map;


public class Configuration {

    Map<String, Object> _config;

    public Configuration(Map config) {
        _config = config;
    }

    public String getString(String key) {
        return (String) _config.get(key);
    }

    public Integer getInt(String key) {
        return (Integer) _config.get(key);
    }

    public Object get(String key) {
        return _config.getOrDefault(key,null);
    }

    public String toString(){
        return _config.toString();
    }
}

