package com.quantiply.rico;

/**
 * Created by arrawatia on 1/27/15.
 */
public interface Context {

    KeyValueStore getKeyValueStore(String name);
}
