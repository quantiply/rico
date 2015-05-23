package com.quantiply.samza.metrics;

public interface StreamMetricFactory<M> {

    M create(StreamMetricRegistry registry);
}
