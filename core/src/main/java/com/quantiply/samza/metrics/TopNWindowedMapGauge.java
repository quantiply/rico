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

public class TopNWindowedMapGauge<V extends Comparable<? super V>> extends WindowedMapGauge<V> {
    private int n;

    public TopNWindowedMapGauge(String name, long windowDurationMs, BiFunction<V, V, V> mergeFunc, int n) {
        super(name, windowDurationMs, mergeFunc);
        this.n = n;
    }

    public TopNWindowedMapGauge(String name, long windowDurationMs, BiFunction<V, V, V> mergeFunc, int n, Clock clock) {
        super(name, windowDurationMs, mergeFunc, clock);
        this.n = n;
    }

    @Override
    public Map<String, V> getSnapshot() {
        Map<String, V> fullMap = super.getSnapshot();
        return topN(fullMap);
    }

    protected Map<String, V> topN(Map<String, V> map)
    {
        Map<String,V> result = new HashMap<>();
        Stream<Map.Entry<String,V>> st = map.entrySet().stream();

        Comparator<Map.Entry<String,V>> comparator = Comparator.comparing(
                (Map.Entry<String, V> e) -> e.getValue())
                .reversed();
        st
                .sorted(comparator)
                .limit(n)
                .forEach(e -> result.put(e.getKey(), e.getValue()));
        return result;
    }
}
