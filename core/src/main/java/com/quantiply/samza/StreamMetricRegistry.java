/*
 * Copyright 2014-2015 Quantiply Corporation. All rights reserved.
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
package com.quantiply.samza;

import com.codahale.metrics.*;

/**
 Utility for creating a set of metrics that share a common prefix
 */
public class StreamMetricRegistry {

    public static String sanitizeStreamNameForMetrics(String streamName) {
        return streamName.replaceAll("\\.", "_");
    }

    private final String namePrefix;
    private final MetricAdaptor metricAdaptor;

    public StreamMetricRegistry(String namePrefix, MetricAdaptor metricAdaptor) {
        this.namePrefix = namePrefix;
        this.metricAdaptor = metricAdaptor;
    }

    public <T extends Metric> T register(String name, T metric) {
        return metricAdaptor.register(getStreamMetricName(name), metric);
    }

    public Histogram histogram(String name) {
        return metricAdaptor.histogram(getStreamMetricName(name));
    }

    public Counter counter(String name) {
        return metricAdaptor.counter(getStreamMetricName(name));
    }

    public Timer timer(String name) {
        return metricAdaptor.timer(getStreamMetricName(name));
    }

    public Meter meter(String name) {
        return metricAdaptor.meter(getStreamMetricName(name));
    }

    public Gauge gauge(String name, Gauge gauge) {
        return metricAdaptor.gauge(getStreamMetricName(name), gauge);
    }

    private String getStreamMetricName(String name) {
        return namePrefix + name;
    }
}
