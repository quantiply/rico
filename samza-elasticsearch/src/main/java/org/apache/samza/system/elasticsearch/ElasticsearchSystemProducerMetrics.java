package org.apache.samza.system.elasticsearch;

import org.apache.samza.metrics.Counter;
import org.apache.samza.metrics.MetricsRegistry;

public class ElasticsearchSystemProducerMetrics {
    public final Counter bulkSendSuccess;
    public final Counter bulkSendFailure;
    public final Counter rejectedMsgs;
    public final Counter unAckedMsgs;
    public final Counter ackedMsgsInFailedBatch;
    public final Counter ackedMsgsInSuccessBatch;
    private final MetricsRegistry registry;
    private final String group;
    private final String systemName;

    public ElasticsearchSystemProducerMetrics(String systemName, MetricsRegistry registry) {
        group = this.getClass().getName();
        this.registry = registry;
        this.systemName = systemName;

        bulkSendSuccess = newCounter("bulk-send-success");
        bulkSendFailure = newCounter("bulk-send-failure");
        ackedMsgsInFailedBatch = newCounter("msgs-acked-in-failed-batch");
        ackedMsgsInSuccessBatch = newCounter("msgs-acked-in-success-batch");
        rejectedMsgs = newCounter("msgs-rejected");
        unAckedMsgs = newCounter("msgs-unacked");
    }

    private Counter newCounter(String name) {
        return registry.newCounter(group, (systemName + "-" + name).toLowerCase());
    }
}
