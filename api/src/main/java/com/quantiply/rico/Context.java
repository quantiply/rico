package com.quantiply.rico;

import com.codahale.metrics.*;

/**
 * Created by arrawatia on 1/27/15.
 */
public interface Context {

    KeyValueStore getKeyValueStore(String name);


    Histogram histogram(String name);
    Counter counter(String name);
    Timer timer(String name);
    Meter meter(String name);
    void gauge(String name, Gauge g);

}
