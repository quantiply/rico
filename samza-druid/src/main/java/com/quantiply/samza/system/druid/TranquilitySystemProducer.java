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

import com.quantiply.druid.HTTPTranquilityLoader;
import org.apache.samza.SamzaException;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemProducer;
import org.apache.samza.util.Clock;
import org.apache.samza.util.SystemClock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.function.Consumer;

public class TranquilitySystemProducer implements SystemProducer {
  private Logger LOGGER = LoggerFactory.getLogger(new Object() {}.getClass().getEnclosingClass());

  private final String systemName;
  private final HTTPTranquilityLoader bulkLoader;
  private final Optional<EventTimeExtractor> eventTsExtractor;

  public TranquilitySystemProducer(String systemName,
                                   HTTPTranquilityLoaderFactory bulkLoaderFactory,
                                   Optional<EventTimeExtractor> eventTsExtractor,
                                   TranquilitySystemProducerMetrics metrics) {
    this.systemName = systemName;
    this.eventTsExtractor = eventTsExtractor;
    this.bulkLoader = bulkLoaderFactory.getBulkLoader(systemName, new FlushListener(metrics, systemName));
  }

  @Override
  public void start() {
    LOGGER.info("Starting Tranquility system producer");
    bulkLoader.start();
  }

  @Override
  public void stop() {
    LOGGER.info("Stopping Tranquility system producer");
    LOGGER.debug("Flushing any remaining actions");
    try {
      flushAll();
    }
    catch (Throwable e) {}
    LOGGER.debug("Stopping the writer thread");
    bulkLoader.stop();
  }

  @Override
  public void register(final String source) {}

  @Override
  public void send(final String source, final OutgoingMessageEnvelope envelope) {
    try {
      Optional<Long> eventTsMs = eventTsExtractor.map(f -> f.getEventTsMs(envelope));
      long receivedTsMs = System.currentTimeMillis();
      byte[] record = (byte[]) envelope.getMessage();
      HTTPTranquilityLoader.IndexRequest req = new HTTPTranquilityLoader.IndexRequest(eventTsMs, receivedTsMs, record);
      bulkLoader.addAction(source, req);
    }
    catch (Throwable e) {
      String message = String.format("Error writing to Tranquility system %s.", systemName);
      LOGGER.error(message, e);
      throw new SamzaException(message, e);
    }
  }

  @Override
  public void flush(final String source) {
    flushAll();
  }

  /**
   * Error contract:
   *    this method will throw an Exception if any non-ignorable errors have occurred
   */
  public void flushAll() {
    try {
      bulkLoader.flush();
      LOGGER.info(String.format("Flushed Tranquility system %s", systemName));
    }
    catch (Throwable e) {
      String message = String.format("Error writing to Tranquility system %s", systemName);
      LOGGER.error(message, e);
      throw new SamzaException(message, e);
    }
  }

  /**
   *
   * Callback for metrics, runs in the writer thread
   *
   * Throws exception for any non-ignorable errors - will stop the producer. Retries are
   * accomplished by restarting the job
   *
   */
  protected static class FlushListener implements Consumer<HTTPTranquilityLoader.BulkReport> {
    private Logger logger = LoggerFactory.getLogger(new Object() {}.getClass().getEnclosingClass());
    protected final TranquilitySystemProducerMetrics metrics;
    protected final String systemName;
    protected final Clock clock;

    public FlushListener(TranquilitySystemProducerMetrics metrics, String systemName) {
      this(metrics, systemName, new SystemClock());
    }

    public FlushListener(TranquilitySystemProducerMetrics metrics, String systemName, Clock clock) {
      this.metrics = metrics;
      this.systemName = systemName;
      this.clock = clock;
    }

    @Override
    public void accept(HTTPTranquilityLoader.BulkReport report) {
      long tsNowMs = clock.currentTimeMillis();
      logger.debug(String.format("Pushed %s actions to Tranquility system %s", report.requests.size(), systemName));
      updateSuccessMetrics(report, tsNowMs);
    }

    protected void updateSuccessMetrics(HTTPTranquilityLoader.BulkReport report, long tsNowMs) {
      metrics.bulkSendSuccess.inc();
      metrics.bulkSendBatchSize.update(report.requests.size());
      metrics.bulkSendWaitMs.update(report.waitMs);
      switch (report.triggerType) {
        case MAX_RECORDS:
          metrics.triggerMaxRecords.inc();
          break;
        case MAX_INTERVAL:
          metrics.triggerMaxInterval.inc();
          break;
        case FLUSH_CMD:
          metrics.triggerFlushCmd.inc();
          break;
      }
      metrics.sent.inc(report.response.sent);
      if (report.response.received > report.response.sent) {
        metrics.dropped.inc(report.response.received - report.response.sent);
      }

      for (HTTPTranquilityLoader.SourcedIndexRequest req: report.requests) {
        metrics.lagFromReceiveMs.update(tsNowMs - req.request.receivedTsMs);
        if (req.request.eventTsMs.isPresent()) {
          metrics.lagFromOriginMs.update(tsNowMs - req.request.eventTsMs.get());
        }
      }
    }

  }

}

