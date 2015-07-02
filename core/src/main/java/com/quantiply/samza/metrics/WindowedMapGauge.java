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
import org.apache.samza.util.Clock;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;

/**
 *
 * A metric for tracking values for a set of keys.  It keeps
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
 * The update method is called from the main event loop thread while
 * getValue() is called from reporter threads.  We're going to do all state
 * changes in the main event loop thread for simplicity and to avoid locking
 *
 */
public class WindowedMapGauge<V> extends Gauge<Map<String,Object>> {
    private final long windowDurationMs;
    private final Clock clock;
    private Map<String,V> curWindowMap;
    private Map<String,V> prevWindowMap;
    private Windows windows;
    private final BiFunction<V,V,V> mergeFunc;

    /**
     *
     * @param name metric name
     * @param windowDurationMs Window size in millisecons
     * @param mergeFunc Function for merging multiple values for the same key
     */
    public WindowedMapGauge(String name, long windowDurationMs, BiFunction<V,V,V> mergeFunc) {
        this(name, windowDurationMs, mergeFunc, System::currentTimeMillis);
    }

    public WindowedMapGauge(String name, long windowDurationMs, BiFunction<V,V,V> mergeFunc, Clock clock) {
        super(name, new HashMap<>());
        assert windowDurationMs > 0L;
        this.windowDurationMs = windowDurationMs;
        this.mergeFunc = mergeFunc;
        this.clock = clock;
        windows = getWindowStartTimes(clock.currentTimeMillis());
        curWindowMap = new HashMap<>();
        prevWindowMap = new HashMap<>();
    }

    /**
     *
     * Called from the main event loop thread. All state changes are done in this thread.
     */
    public void update(String src, V val){
        Windows newWindows = getWindowStartTimes(clock.currentTimeMillis());
        if (!newWindows.equals(windows)) {
            prevWindowMap = newWindows.prevStartMs == windows.activeStartMs? curWindowMap : new HashMap<>();
            curWindowMap = new HashMap<>();
            windows = newWindows;
        }
        curWindowMap.merge(src, val, mergeFunc);
    }

    /**
     *
     * This method is called by metric reporter threads. It's not strictly thread-safe
     * but in the worst case reports the previous value.
     */
    @Override
    public Map<String,Object> getValue() {
        /*
        Note that windows can only update every windowDurationMs and they only update
        in one direction (increasing time)

        The worst case for this race condition is that we are off by one update.
         */
        Windows newWindows = getWindowStartTimes(clock.currentTimeMillis());
        //Copying this here to minimize race condition
        long activeStartMs = windows.activeStartMs;

        Map<String,Object> data = new HashMap<>();
        //Check against both current and previous start times in case we had a race condition
        if (newWindows.activeStartMs == activeStartMs || newWindows.prevStartMs == activeStartMs) {
            data = Collections.unmodifiableMap(prevWindowMap);
        }

        Map<String,Object> value = new HashMap<>();
        value.put("type", "windowed-map");
        value.put("window-duration-ms", windowDurationMs);
        value.put("data", data);
        return value;
    }

    public Windows getWindowStartTimes(long tsMs) {
        long activeStartMs = tsMs/windowDurationMs * windowDurationMs;
        return new Windows(activeStartMs, activeStartMs - windowDurationMs);
    }

    public static class Windows {
        public final long activeStartMs;
        public final long prevStartMs;

        public Windows(long activeStartMs, long prevStartMs) {
            this.activeStartMs = activeStartMs;
            this.prevStartMs = prevStartMs;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Windows windows = (Windows) o;
            return Objects.equals(activeStartMs, windows.activeStartMs) &&
                    Objects.equals(prevStartMs, windows.prevStartMs);
        }

        @Override
        public int hashCode() {
            return Objects.hash(activeStartMs, prevStartMs);
        }

        @Override
        public String toString() {
            return "Windows{" +
                    "activeStartMs=" + activeStartMs +
                    ", prevStartMs=" + prevStartMs +
                    '}';
        }
    }
}
