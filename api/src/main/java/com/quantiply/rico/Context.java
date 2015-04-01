package com.quantiply.rico;

public interface Context {

    KeyValueStore getKeyValueStore(String name);

    Metrics metrics(String group);
}
