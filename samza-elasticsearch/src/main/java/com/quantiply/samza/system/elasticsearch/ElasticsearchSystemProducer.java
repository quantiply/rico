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
 * Each system that is configured in Samza has an independent {@link HTTPBulkLoader} that flush
 * separably to Elasticsearch. Each {@link HTTPBulkLoader} will maintain the ordering of messages
 * being sent from tasks per Samza container. If you have multiple containers writing to the same
 * message id there is no guarantee of ordering in Elasticsearch.
 * </p>
 *
 * <p>
 * This can be fully configured from the Samza job properties.
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
    this.bulkLoader = bulkLoaderFactory.getBulkLoader(client);
    this.metrics = metrics;
  }


  @Override
  public void start() {
    // Nothing to do.
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
  public void send(String source, OutgoingMessageEnvelope envelope) {
    try {
      bulkLoader.addAction(source, msgToAction.apply(envelope));
    } catch (IOException e) {
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
    catch (Exception e) {
      String message = String.format("Error writing to Elasticsearch system %s.", systemName);
      LOGGER.error(message, e);
      throw new SamzaException(message, e);
    }
  }

}
