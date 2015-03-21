package com.quantiply.rico.api;

import java.util.List;
import java.util.Map;

/**
 * Created by arrawatia on 1/27/15.
 */
public interface KeyValueStore<K,V> {

     V get(K key);
     List<V> get(List<K> keys);


     void remove(K key);
     void remove(List<K> keys);


     void put(K key, V value);
     void putAll(Map<K,V> pairs);


}
