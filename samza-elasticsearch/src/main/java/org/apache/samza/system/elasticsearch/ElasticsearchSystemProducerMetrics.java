package org.apache.samza.system.elasticsearch;

import org.apache.samza.metrics.Counter;
import org.apache.samza.metrics.MetricsRegistry;

public class ElasticsearchSystemProducerMetrics {
    public final Counter bulkSendSuccess;
    public final Counter inserts;
    public final Counter updates;
    private final MetricsRegistry registry;
    private final String group;
    private final String systemName;

    public ElasticsearchSystemProducerMetrics(String systemName, MetricsRegistry registry) {
        group = this.getClass().getName();
        this.registry = registry;
        this.systemName = systemName;

        bulkSendSuccess = newCounter("bulk-send-success");
        inserts = newCounter("docs-inserted");
        updates = newCounter("docs-updated");
    }

    private Counter newCounter(String name) {
        return registry.newCounter(group, (systemName + "-" + name).toLowerCase());
    }
}
