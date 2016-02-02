package com.quantiply.samza.system.elasticsearch;

import org.apache.samza.metrics.Counter;
import org.apache.samza.metrics.MetricsRegistry;

public class ElasticsearchSystemProducerMetrics {
    public final Counter bulkSendSuccess;
    public final Counter docsCreated;
    public final Counter docsIndexed;
    public final Counter docsUpdated;
    public final Counter docsDeleted;
    public final Counter conflicts;
    private final MetricsRegistry registry;
    private final String group;
    private final String systemName;

    public ElasticsearchSystemProducerMetrics(String systemName, MetricsRegistry registry) {
        group = this.getClass().getName();
        this.registry = registry;
        this.systemName = systemName;

        bulkSendSuccess = newCounter("bulk-send-success");
        docsCreated = newCounter("docs-created");
        docsIndexed = newCounter("docs-indexed");
        docsUpdated = newCounter("docs-updated");
        docsDeleted = newCounter("docs-deleted");
        conflicts = newCounter("version-conflicts");
    }

    private Counter newCounter(String name) {
        return registry.newCounter(group, (systemName + "-" + name).toLowerCase());
    }
}
