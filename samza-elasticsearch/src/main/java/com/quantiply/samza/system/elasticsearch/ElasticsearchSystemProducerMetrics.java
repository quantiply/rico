package com.quantiply.samza.system.elasticsearch;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.quantiply.samza.ConfigConst;
import com.quantiply.samza.MetricAdaptor;
import org.apache.samza.metrics.Counter;
import org.apache.samza.metrics.MetricsRegistry;

public class ElasticsearchSystemProducerMetrics {
    public final Counter bulkSendSuccess;
    public final Histogram batchSize;
    public final Counter triggerFlushCmd;
    public final Counter triggerMaxActions;
    public final Counter triggerMaxInterval;
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

        //TODO - fix naming here
        MetricAdaptor adaptor = new MetricAdaptor(new MetricRegistry(), registry, ConfigConst.METRICS_GROUP_NAME);
        
        bulkSendSuccess = newCounter("bulk-send-success");
        batchSize = adaptor.histogram("bulk-send-batch-size");
        triggerFlushCmd = newCounter("bulk-send-trigger-flush-cmd");
        triggerMaxActions = newCounter("bulk-send-trigger-max-actions");
        triggerMaxInterval = newCounter("bulk-send-trigger-max-interval");
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
