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

import org.apache.samza.metrics.Gauge;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;

/**
 *
 * A class for tracking values for a set of keys.  It keeps
 * an active window the current time bucket and reports
 * the previous time bucket values to metric visitors.
 *
 * An example use for this class is tracking the maximum lag
 * per data source.  At the end of the active bucket,
 * the maximum value per source during for that period will be reported.
 *
 * Buckets are based on wall-clock time. Ideally the window duration should
 * match the metric reporter frequency.
 *
 * At any point, we need to know what the active and last buckets should be,
 * whether I have either one, and get into the right state
 *
 */
public class WindowedMapGauge<V> extends Gauge<Map<String,Object>> {
    private final long windowDurationMs;
    private ConcurrentHashMap<String,V> curWindowMap;
    private ConcurrentHashMap<String,V> prevWindowMap;
    private long windowStartMs;
    private final BiFunction<V,V,V> mergeFunc;

    public WindowedMapGauge(String name, long windowDurationMs, BiFunction<V,V,V> mergeFunc) {
        super(name, new ConcurrentHashMap<>());
        this.windowDurationMs = windowDurationMs;
        this.mergeFunc = mergeFunc;
        curWindowMap = new ConcurrentHashMap<>();
        prevWindowMap = new ConcurrentHashMap<>();
        windowStartMs = System.currentTimeMillis();
    }

    public void update(String src, V val){
        curWindowMap.merge(src, val, mergeFunc);
    }

    @Override
    public Map<String,Object> getValue() {
        Map<String,Object> data = new HashMap<>();
        data.put("type", "windowed-map");
        data.put("window-duration-ms", windowDurationMs);
        data.put("data", Collections.unmodifiableMap(prevWindowMap));
        return data;
    }

    private synchronized void checkDurationAndReset(long tsNow) {
        if (tsNow - windowStartMs > windowDurationMs) {
            prevWindowMap = curWindowMap;
            curWindowMap = new ConcurrentHashMap<>();
            windowStartMs = tsNow;
        }
    }
}
