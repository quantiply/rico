/*
 * Copyright 2016 Quantiply Corporation. All rights reserved.
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
  public final Counter sent;
  public final Counter dropped;
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
    sent = newCounter("sent");
    dropped = newCounter("dropped");
  }

  private Histogram newHistogram(MetricAdaptor adaptor, String name) {
    return adaptor.histogram((systemName + "-" + name).toLowerCase());
  }

  private Counter newCounter(String name) {
    return registry.newCounter(group, (systemName + "-" + name).toLowerCase());
  }
}
