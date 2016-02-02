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

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
  private Logger LOGGER = LoggerFactory.getLogger(new Object() {}.getClass().getEnclosingClass());

  private final String systemName;
  private final HTTPBulkLoader bulkLoader;
  private final JestClient client;
  private final Function<OutgoingMessageEnvelope, HTTPBulkLoader.ActionRequest> msgToAction;

  public ElasticsearchSystemProducer(String systemName,
                                     HTTPBulkLoaderFactory bulkLoaderFactory,
                                     JestClient client,
                                     Function<OutgoingMessageEnvelope,HTTPBulkLoader.ActionRequest> msgToAction,
                                     ElasticsearchSystemProducerMetrics metrics) {
    this.systemName = systemName;
    this.client = client;
    this.msgToAction = msgToAction;
    this.bulkLoader = bulkLoaderFactory.getBulkLoader(client, new FlushListener(metrics, systemName));
  }

  @Override
  public void start() {
    LOGGER.info("Starting Elasticsearch writer thread");
    bulkLoader.start();
  }

  @Override
  public void stop() {
    LOGGER.info("Stopping Elasticsearch system producer");
    LOGGER.debug("Flushing any remaining actions");
    flushAll();
    LOGGER.debug("Stopping the writer thread");
    bulkLoader.stop();
    LOGGER.debug("Closing the connection");
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

//  2016-02-01 23:19:11 ElasticsearchSystemProducer [DEBUG] task[] ssp[] offset[] op index, index embedded/test, status 201, id testIndex, error null
//      2016-02-01 23:19:11 ElasticsearchSystemProducer [DEBUG] task[] ssp[] offset[] op index, index embedded/test, status 200, id testIndex, error null
//      2016-02-01 23:19:11 ElasticsearchSystemProducer [DEBUG] task[] ssp[] offset[] op index, index embedded/test, status 201, id testVC, error null
//      2016-02-01 23:19:11 ElasticsearchSystemProducer [DEBUG] task[] ssp[] offset[] op index, index embedded/test, status 409, id testVC, error {"type":"version_conflict_engine_exception","reason":"[test][testVC]: version conflict, current [123], provided [122]","shard":"2","index":"embedded"}
//  2016-02-01 23:19:11 ElasticsearchSystemProducer [DEBUG] task[] ssp[] offset[] op index, index embedded/test, status 400, id testMappingEx, error {"type":"merge_mapping_exception","reason":"Merge fai

  /**
   *
   * Callback for ES metrics, runs in the writer thread
   *
   * Throws exception for any non-ignorable errors - will stop the producer. Retries are
   * accomplished by restarting the job
   *
   */
  protected class FlushListener implements Consumer<HTTPBulkLoader.BulkReport> {
    private Logger logger = LoggerFactory.getLogger(new Object() {}.getClass().getEnclosingClass());
    protected final int STATUS_CONFLICT = 409;
    protected final ElasticsearchSystemProducerMetrics metrics;
    protected final String systemName;

    public FlushListener(ElasticsearchSystemProducerMetrics metrics, String systemName) {
      this.metrics = metrics;
      this.systemName = systemName;
    }

    @Override
    public void accept(HTTPBulkLoader.BulkReport report) {
      BulkResult result = report.bulkResult;
      if (!result.isSucceeded()) {
        if (result.getItems().size() == 0) {
          throw new SamzaException("Elasticsearch API error: " + result.getErrorMessage());
        }
        //Ignore version conflicts
        List<BulkResult.BulkResultItem> fatal = result.getFailedItems().stream().filter(item -> item.status != STATUS_CONFLICT).collect(Collectors.toList());
            ;
        if (fatal.size() > 0) {
          fatal.forEach(item -> logger.error(String.format("Error: index %s/%s, id %s, status %s, error %s",
              item.index, item.type, item.id, item.status, item.error)));
          throw new SamzaException(String.format("Elasticsearch bulk result contained %s errors", fatal.size()));
        }
      }
      logger.debug(String.format("Wrote %s actions to Elasticsearch system %s", result.getItems().size(), systemName));
      updateSuccessMetrics(report);
    }

    protected void updateSuccessMetrics(HTTPBulkLoader.BulkReport report) {
      metrics.bulkSendSuccess.inc();
      for (BulkResult.BulkResultItem item : report.bulkResult.getItems()) {
        if (item.status == STATUS_CONFLICT) {
          metrics.conflicts.inc();
        }
        else {
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
      }
    }

  }

}
