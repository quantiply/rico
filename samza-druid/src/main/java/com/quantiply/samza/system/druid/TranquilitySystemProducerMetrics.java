package com.quantiply.samza.system.druid;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.quantiply.samza.MetricAdaptor;
import org.apache.samza.metrics.Counter;
import org.apache.samza.metrics.MetricsRegistry;

public class TranquilitySystemProducerMetrics {
  public final Counter bulkSendSuccess;
  public final Histogram bulkSendBatchSize;
  public final Histogram bulkSendWaitMs;
  public final Counter triggerFlushCmd;
  public final Counter triggerMaxRecords;
  public final Counter triggerMaxInterval;
  public final Histogram lagFromReceiveMs;
  public final Histogram lagFromOriginMs;
  public final Counter received;
  public final Counter sent;
  private final MetricsRegistry registry;
  private final String group;
  private final String systemName;

  public TranquilitySystemProducerMetrics(String systemName, MetricsRegistry registry) {
    group = this.getClass().getName();
    this.registry = registry;
    this.systemName = systemName;

    MetricAdaptor adaptor = new MetricAdaptor(new MetricRegistry(), registry, group);

    bulkSendSuccess = newCounter("bulk-send-success");
    bulkSendBatchSize = newHistogram(adaptor, "bulk-send-batch-size");
    bulkSendWaitMs = newHistogram(adaptor, "bulk-send-wait-ms");
    triggerFlushCmd = newCounter("bulk-send-trigger-flush-cmd");
    triggerMaxRecords = newCounter("bulk-send-trigger-max-records");
    triggerMaxInterval = newCounter("bulk-send-trigger-max-interval");
    lagFromReceiveMs = newHistogram(adaptor, "lag-from-receive-ms");
    lagFromOriginMs = newHistogram(adaptor, "lag-from-origin-ms");
    received = newCounter("received");
    sent = newCounter("sent");
  }

  private Histogram newHistogram(MetricAdaptor adaptor, String name) {
    return adaptor.histogram((systemName + "-" + name).toLowerCase());
  }

  private Counter newCounter(String name) {
    return registry.newCounter(group, (systemName + "-" + name).toLowerCase());
  }
}
