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
package com.quantiply.elasticsearch;

import com.quantiply.rico.elasticsearch.Action;
import com.quantiply.rico.elasticsearch.ActionRequestKey;
import io.searchbox.action.BulkableAction;
import io.searchbox.client.JestClient;
import io.searchbox.core.Bulk;
import io.searchbox.core.BulkResult;
import io.searchbox.core.DocumentResult;
import io.searchbox.core.Index;
import io.searchbox.params.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class HTTPBulkLoader {

  public static class Config {
    public final int flushMaxActions;
    public final Optional<Integer> flushMaxIntervalMs;

    public Config(int flushMaxActions, Optional<Integer> flushMaxIntervalMs) {
      this.flushMaxActions = flushMaxActions;
      this.flushMaxIntervalMs = flushMaxIntervalMs;
    }
  }

  public enum TriggerType { MAX_ACTIONS, MAX_INTERVAL, FLUSH_CALL }

  public static class BulkReport {
    public final BulkResult bulkResult;
    public final TriggerType triggerType;
    public final List<SourcedActionRequest> requests;

    public BulkReport(BulkResult bulkResult, TriggerType triggerType, List<SourcedActionRequest> requests) {
      this.bulkResult = bulkResult;
      this.triggerType = triggerType;
      this.requests = requests;
    }
  }

  public static class ActionRequest {
    public final ActionRequestKey key;
    public final String index;
    public final String docType;
    public final long receivedTsMs;
    //rhoover - would be better if we could use byte[] but Jest only supports String or Object
    public final String document;

    public ActionRequest(ActionRequestKey key, String index, String docType, long receivedTsMs, String document) {
      this.key = key;
      this.index = index;
      this.docType = docType;
      this.receivedTsMs = receivedTsMs;
      this.document = document;
    }
  }

  public static class SourcedActionRequest {
    public final ActionRequest request;
    public final String source;

    public SourcedActionRequest(String source, ActionRequest request) {
      this.request = request;
      this.source = source;
    }
  }

  protected final Config config;
  protected final JestClient client;
  protected final List<BulkableAction<DocumentResult>> actions;
  protected final List<SourcedActionRequest> requests;
  protected long lastFlushTsMs = System.currentTimeMillis();
  protected Logger logger = LoggerFactory.getLogger(new Object(){}.getClass().getEnclosingClass());

  //This class is the interface to the outside and runs in main/client thread
  public HTTPBulkLoader(Config config, JestClient client) {
    this.config = config;
    this.client = client;
    actions = new ArrayList<>();
    requests = new ArrayList<>();
  }

  /**
   * Convert requests to JEST API objects and pass to writer thread
   *
   * @param req
   * @throws IOException
     */
  public void addAction(String source, ActionRequest req) {
    BulkableAction<DocumentResult> action = null;
    if (req.key.getAction().equals(Action.INDEX)) {
      Index.Builder builder = new Index.Builder(req.document)
          .id(req.key.getId().toString())
          .index(req.index)
          .type(req.docType);
      if (req.key.getVersionType() != null) {
        builder.setParameter(Parameters.VERSION_TYPE, req.key.getVersionType().toString());
      }
      if (req.key.getVersion() != null) {
        builder.setParameter(Parameters.VERSION, req.key.getVersion());
      }
      action = builder.build();
    }
    else {
      throw new RuntimeException("Not implemented");
    }
    actions.add(action);
    requests.add(new SourcedActionRequest(source, req));
//    checkFlush(req.receivedTsMs); //TODO - is this the right time??
  }

  /**
   * Issue flush request to writer thread and block until complete
   *
   * Error contract:
   *    this method will throw an Exception if any non-ignorable errors occur in the writer thread
   */
  public BulkReport flush() {
    return flush(TriggerType.FLUSH_CALL);
  }

  protected BulkReport flush(TriggerType triggerType) throws IOException {
    Bulk bulkRequest = new Bulk.Builder().addAction(actions).build();
    BulkReport report = null;
    try {
      BulkResult bulkResult = client.execute(bulkRequest);
      report = new BulkReport(bulkResult, triggerType, requests);
    }
    catch (Exception e) {
      logger.error("Error writing to Elasticsearch", e);
      throw e;
    }
    finally {
      actions.clear();
      requests.clear();
      lastFlushTsMs = System.currentTimeMillis();
    }
    return report;
  }

  /**
   * Signal writer thread to shutdown
   */
  public void close() {
    client.shutdownClient();
  }

  protected TriggerType getTrigger(long tsNowMs) {
    if (actions.size() >= config.flushMaxActions) {
      return TriggerType.MAX_ACTIONS;
    }
    if (config.flushMaxIntervalMs.isPresent()) {
      long msSinceFlush = tsNowMs - lastFlushTsMs;
      if (msSinceFlush > config.flushMaxIntervalMs.get()) {
        return TriggerType.MAX_INTERVAL;
      }
    }
    return null;
  }

  protected void checkFlush(long tsNowMs) throws IOException {
    TriggerType triggerType = getTrigger(tsNowMs);
    if (triggerType != null) {
      flush(triggerType);
    }
  }

  //everything in writer thread happens here
  protected class Writer {

  }

}
