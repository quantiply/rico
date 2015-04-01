package com.quantiply.rico.samza;

import com.codahale.metrics.*;
import com.quantiply.rico.Metrics;
import com.quantiply.samza.MetricAdaptor;
import org.apache.samza.metrics.MetricsRegistry;

public class SamzaMetrics extends MetricAdaptor implements Metrics {

    public SamzaMetrics(MetricRegistry codaRegistry, MetricsRegistry samzaRegistry, String groupName) {
        super(codaRegistry, samzaRegistry, groupName);
    }
}
