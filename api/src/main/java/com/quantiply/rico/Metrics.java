package com.quantiply.rico;

import com.codahale.metrics.*;

public interface Metrics {
    <T extends Metric> T register(String name, T metric);
    Histogram histogram(String name);
    Counter counter(String name);
    Timer timer(String name);
    Meter meter(String name);
    Gauge gauge(String name, Gauge gauge);
}