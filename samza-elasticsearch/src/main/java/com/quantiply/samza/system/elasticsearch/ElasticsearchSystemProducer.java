/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.quantiply.samza.system.elasticsearch;

import com.quantiply.elasticsearch.HTTPBulkLoader;
import io.searchbox.client.JestClient;
import io.searchbox.core.BulkResult;
import org.apache.samza.SamzaException;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

/** A {@link SystemProducer} for Elasticsearch that builds on top of the {@link HTTPBulkLoader}
 *
 * <p>
 * Each Samza system in the config has an independent {@link HTTPBulkLoader} that flushes
 * separably to Elasticsearch. Each {@link HTTPBulkLoader} will maintain the ordering of messages
 * being sent from tasks per Samza container. If you have multiple containers writing to the same
 * message id there is no guarantee of ordering in Elasticsearch.
 * </p>
 *
 * <p>
 * Samza calls flush() each task separately but this system producer flushes all tasks together
 * to avoid the extra machinery of a bulk loader per task, each with their own writer threads.
 * </p>
 *
 * */
public class ElasticsearchSystemProducer implements SystemProducer {
  private static final Logger LOGGER = LoggerFactory.getLogger(ElasticsearchSystemProducer.class);

  private final String systemName;
  private final HTTPBulkLoader bulkLoader;
  private final JestClient client;

  private final ElasticsearchSystemProducerMetrics metrics;
  private final Function<OutgoingMessageEnvelope, HTTPBulkLoader.ActionRequest> msgToAction;

  public ElasticsearchSystemProducer(String systemName,
                                     HTTPBulkLoaderFactory bulkLoaderFactory,
                                     JestClient client,
                                     Function<OutgoingMessageEnvelope,HTTPBulkLoader.ActionRequest> msgToAction,
                                     ElasticsearchSystemProducerMetrics metrics) {
    this.systemName = systemName;
    this.client = client;
    this.msgToAction = msgToAction;
    this.bulkLoader = bulkLoaderFactory.getBulkLoader(client, this::onFlush);
    this.metrics = metrics;
  }

  /**
   *
   * Callback for ES metrics, runs in the writer thread
   *
   */
  public void onFlush(HTTPBulkLoader.BulkReport report) {
    LOGGER.debug("ON FLUSH");
    try {
      LOGGER.debug("Succeeded" + report.bulkResult.isSucceeded());
      LOGGER.debug(report.bulkResult.getErrorMessage());
      metrics.bulkSendSuccess.inc();
      for (BulkResult.BulkResultItem item : report.bulkResult.getItems()) {
        LOGGER.debug(String.format("op %s, type %s, status %s, id %s", item.operation, item.type, item.status, item.id));
        switch (item.operation) {
          case "index":
            metrics.docsIndexed.inc();
            break;
          case "create":
            metrics.docsCreated.inc();
            break;
          case "update":
            metrics.docsUpdated.inc();
            break;
          case "delete":
            metrics.docsDeleted.inc();
          default:
            break;
        }
      }
    } catch (Exception e) {
      LOGGER.error("Error updating metrics", e);
    }
  }

  @Override
  public void start() {
    bulkLoader.start();
  }

  @Override
  public void stop() {
    flushAll();
    bulkLoader.stop();
    client.shutdownClient();
  }

  @Override
  public void register(final String source) {
    //TODO - create metrics per source??
  }

  @Override
  public void send(final String source, final OutgoingMessageEnvelope envelope) {
    try {
      bulkLoader.addAction(source, msgToAction.apply(envelope));
    }
    catch (Throwable e) {
      String message = String.format("Error writing to Elasticsearch system %s.", systemName);
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
      LOGGER.info(String.format("Flushed Elasticsearch system: %s.", systemName));
    }
    catch (Throwable e) {
      String message = String.format("Error writing to Elasticsearch system %s.", systemName);
      LOGGER.error(message, e);
      throw new SamzaException(message, e);
    }
  }

}
