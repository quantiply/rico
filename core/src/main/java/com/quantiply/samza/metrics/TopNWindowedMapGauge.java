/*
 * Copyright 2015 Quantiply Corporation. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.quantiply.samza.metrics;

import org.apache.samza.util.Clock;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.stream.Stream;

public class TopNWindowedMapGauge<V> extends WindowedMapGauge<V> {
    private int n = 10;

    public TopNWindowedMapGauge(String name, long windowDurationMs, BiFunction<V, V, V> mergeFunc) {
        super(name, windowDurationMs, mergeFunc);
    }

    public TopNWindowedMapGauge(String name, long windowDurationMs, BiFunction<V, V, V> mergeFunc, Clock clock) {
        super(name, windowDurationMs, mergeFunc, clock);
    }

    @Override
    public Map<String, Object> getValue() {
        Map<String, Object> fullMap = super.getValue();
        return fullMap;
    }

    /*
    Based on: http://stackoverflow.com/questions/109383/how-to-sort-a-mapkey-value-on-the-values-in-java
     */
    protected <K, V extends Comparable<? super V>> Map<K, V> sortByValue(Map<K, V> map)
    {
        Map<K,V> result = new HashMap<>();
        Stream<Map.Entry<K,V>> st = map.entrySet().stream();

        Comparator<Map.Entry<K,V>> comparator = Comparator.comparing((Map.Entry<K, V> e) -> e.getValue()).reversed();
        st
                .sorted(comparator)
                .limit(n)
                .forEach(e -> result.put(e.getKey(), e.getValue()));
        return result;
    }
}
