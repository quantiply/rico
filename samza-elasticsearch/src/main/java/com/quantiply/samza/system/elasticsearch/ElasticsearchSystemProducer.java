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
import com.quantiply.samza.system.elasticsearch.indexrequest.IndexRequestFactory;
import io.searchbox.client.JestClient;
import org.apache.samza.SamzaException;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemProducer;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.rest.RestStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
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

  private final String system;
  private final Map<String, HTTPBulkLoader> sourceBulkLoader;
  private final AtomicBoolean sendFailed = new AtomicBoolean(false);
  private final AtomicReference<Throwable> thrown = new AtomicReference<>();

  private final ElasticsearchSystemProducerMetrics metrics;

  public ElasticsearchSystemProducer(String system,
                                     HTTPBulkLoaderFactory bulkLoaderFactory,
                                     JestClient client,
                                     Function<OutgoingMessageEnvelope,HTTPBulkLoader.ActionRequest> msgToAction,
                                     ElasticsearchSystemProducerMetrics metrics) {
    this.system = system;
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
    //Get config
    //Create callback
    sourceBulkLoader.put(source, new HTTPBulkLoader(null, null));
    //sourceBulkLoader.put(source, bulkProcessorFactory.getBulkProcessor(client, listener));
  }

  @Override
  public void send(String source, OutgoingMessageEnvelope envelope) {
    //IndexRequest indexRequest = indexRequestFactory.getIndexRequest(envelope);
    HTTPBulkLoader.ActionRequest req = null;
    sourceBulkLoader.get(source).addAction(req);
  }

  @Override
  public void flush(String source) {
    try {
      sourceBulkLoader.get(source).flush();
    }
    catch (IOException e) {
      String message = String.format("Unable to send message from %s to system %s.", source,
          system);
      LOGGER.error(message);
      throw new SamzaException(message, e);
    }

    if (sendFailed.get()) {
      String message = String.format("Unable to send message from %s to system %s.", source,
                                     system);
      LOGGER.error(message);

      Throwable cause = thrown.get();
      if (cause != null) {
        throw new SamzaException(message, cause);
      } else {
        throw new SamzaException(message);
      }
    }

    LOGGER.info(String.format("Flushed %s to %s.", source, system));
  }

}
