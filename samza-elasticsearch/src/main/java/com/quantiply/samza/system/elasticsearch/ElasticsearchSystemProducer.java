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
 * Each systemName that is configured in Samza has an independent {@link HTTPBulkLoader} that flush
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
  private final Map<String, HTTPBulkLoader> sourceBulkLoader;
  private final JestClient client;

  private final ElasticsearchSystemProducerMetrics metrics;
  private final HTTPBulkLoaderFactory bulkLoaderFactory;
  private final Function<OutgoingMessageEnvelope, HTTPBulkLoader.ActionRequest> msgToAction;

  public ElasticsearchSystemProducer(String systemName,
                                     HTTPBulkLoaderFactory bulkLoaderFactory,
                                     JestClient client,
                                     Function<OutgoingMessageEnvelope,HTTPBulkLoader.ActionRequest> msgToAction,
                                     ElasticsearchSystemProducerMetrics metrics) {
    this.systemName = systemName;
    this.bulkLoaderFactory = bulkLoaderFactory;
    this.client = client;
    this.msgToAction = msgToAction;
    this.sourceBulkLoader = new HashMap<>();
    this.metrics = metrics;
  }


  @Override
  public void start() {
    // Nothing to do.
  }

  @Override
  public void stop() {
    for (Map.Entry<String, HTTPBulkLoader> e : sourceBulkLoader.entrySet()) {
      flush(e.getKey());
      e.getValue().close();
    }
  }

  @Override
  public void register(final String source) {
    sourceBulkLoader.put(source, bulkLoaderFactory.getBulkLoader(client));
  }

  @Override
  public void send(String source, OutgoingMessageEnvelope envelope) {
    sourceBulkLoader.get(source).addAction(msgToAction.apply(envelope));
  }

  @Override
  public void flush(String source) {
    try {
      sourceBulkLoader.get(source).flush();
    }
    catch (IOException e) {
      String message = String.format("Unable to send message from %s to systemName %s.", source,
          systemName);
      LOGGER.error(message);
      throw new SamzaException(message, e);
    }

//    if (sendFailed.get()) {
//      String message = String.format("Unable to send message from %s to systemName %s.", source,
//          systemName);
//      LOGGER.error(message);
//
//      Throwable cause = thrown.get();
//      if (cause != null) {
//        throw new SamzaException(message, cause);
//      } else {
//        throw new SamzaException(message);
//      }
//    }

    LOGGER.info(String.format("Flushed %s to %s.", source, systemName));
  }

}
