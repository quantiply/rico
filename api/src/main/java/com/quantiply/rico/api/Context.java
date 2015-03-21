package com.quantiply.rico.api;

import com.quantiply.rico.api.KeyValueStore;

/**
 * Created by arrawatia on 1/27/15.
 */
public interface Context {

    KeyValueStore getKeyValueStore(String name);
}
