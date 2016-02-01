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
import java.util.concurrent.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class HTTPBulkLoader {

  public static class Config {
    public final int flushMaxActions;
    public final Optional<Integer> flushMaxIntervalMs;

    public Config(int flushMaxActions, Optional<Integer> flushMaxIntervalMs) {
      this.flushMaxActions = flushMaxActions;
      this.flushMaxIntervalMs = flushMaxIntervalMs;
    }
  }

  public enum TriggerType { MAX_ACTIONS, MAX_INTERVAL, FLUSH_CMD }

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
    public final BulkableAction<DocumentResult> action;
    public final String source;

    public SourcedActionRequest(String source, ActionRequest request, BulkableAction<DocumentResult> action) {
      this.request = request;
      this.source = source;
      this.action = action;
    }
  }

  protected static enum WriterCommandType { ADD_ACTION, FLUSH }

  protected static class WriterCommand {

    public static WriterCommand getAddCmd(SourcedActionRequest req) {
      return new WriterCommand(WriterCommandType.ADD_ACTION, req, null);
    }

    public static WriterCommand getFlushCmd() {
      return new WriterCommand(WriterCommandType.FLUSH, null, new CompletableFuture<Void>());
    }

    public WriterCommand(WriterCommandType type, SourcedActionRequest request, CompletableFuture<Void> flushCompletedFuture) {
      this.type = type;
      this.request = request;
      this.flushCompletedFuture = flushCompletedFuture;
    }

    public final WriterCommandType type;
    public final SourcedActionRequest request;
    public final CompletableFuture<Void> flushCompletedFuture;
  }

  protected static final int SHUTDOWN_WAIT_MS = 100;
  protected final Writer writer;
  protected final ArrayBlockingQueue<WriterCommand> writerCmdQueue;
  protected final ExecutorService writerExecSvc;

  /**
   * Elasticsearch HTTP Bulk Loader
   *
   * Methods in this class run in the client's thread
   *
   * JEST client's lifecycle is manage externally (i.e. must be closed elsewhere)
   * so that it may be shared by multiple instances
   */
  public HTTPBulkLoader(Config config, JestClient client, Optional<Consumer<BulkReport>> onFlushOpt) {
    this.writerCmdQueue = new ArrayBlockingQueue<>(config.flushMaxActions);
    this.writerExecSvc = Executors.newFixedThreadPool(1);
    this.writer = new Writer(config, client, writerCmdQueue, onFlushOpt);
  }

  /**
   * Convert requests to JEST API objects and pass to writer thread
   *
   * May block if internal buffer is full
   */
  public void addAction(String source, ActionRequest req) throws IOException {
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
      //TODO - finish me
      throw new RuntimeException("Not implemented");
    }

    WriterCommand addCmd = WriterCommand.getAddCmd(new SourcedActionRequest(source, req, action));
    try {
      //May block if queue is full
      writerCmdQueue.put(addCmd);
    }
    catch (InterruptedException firstEx) {
      /* If the main Samza thread is interrupted, it's likely a shutdown command
        Try for a clean shutdown by waiting a little longer to enqueue the message
       */
      try {
        if (!writerCmdQueue.offer(addCmd, SHUTDOWN_WAIT_MS, TimeUnit.MILLISECONDS)) {
         throw new IOException("Timed out trying to pass message to Elasticsearch writer on shutdown");
        }
      }
      catch (InterruptedException e) {
        throw new IOException("Interrupted passing message to Elasticsearch writer", e);
      }
    }
  }

  /**
   * Issue flush request to writer thread and block until complete
   *
   * Error contract:
   *    this method will throw an Exception if any non-ignorable errors occur in the writer thread
   */
  public void flush() throws IOException {
    WriterCommand flushCmd = WriterCommand.getFlushCmd();
    try {
      flushCmd.flushCompletedFuture.get();
    } catch (InterruptedException e) {
      /* If the main Samza thread is interrupted, it's likely a shutdown command
        Try for a clean shutdown by waiting a little longer on the flush
       */
      try {
        flushCmd.flushCompletedFuture.get(SHUTDOWN_WAIT_MS, TimeUnit.MILLISECONDS);
      } catch (Exception retryEx) {
        throw new IOException("Error trying to flush to Elasticsearch on shutdown", e);
      }
    } catch (ExecutionException e) {
      throw new IOException("Error writing to Elasticsearch", e.getCause());
    }
  }

  public void start() {
    writerExecSvc.submit(writer);
  }

  /**
   * Signal writer thread to shutdown
   */
  public void stop() {
    writerExecSvc.shutdown();
  }

  /**
   *
   * Writer thread
   *
   */
  protected class Writer implements Runnable {
    protected final Config config;
    protected final JestClient client;
    protected final Optional<Consumer<BulkReport>> onFlushOpt;
    protected final BlockingQueue<WriterCommand> cmdQueue;
    protected long lastFlushTsMs;
    protected Throwable error = null;
    protected final List<WriterCommand> requests;
    protected Logger logger = LoggerFactory.getLogger(new Object(){}.getClass().getEnclosingClass());

    public Writer(Config config, JestClient client, BlockingQueue<WriterCommand> cmdQueue, Optional<Consumer<BulkReport>> onFlushOpt) {
      this.config = config;
      this.cmdQueue = cmdQueue;
      this.client = client;
      this.onFlushOpt = onFlushOpt;
      this.requests = new ArrayList<>(config.flushMaxActions);
    }

    protected WriterCommand poll() throws InterruptedException {
      if (config.flushMaxIntervalMs.isPresent()) {
        long msSinceLastFlush = System.currentTimeMillis() - lastFlushTsMs;
        long msUntilFlush = Math.max(0, config.flushMaxIntervalMs.get().longValue() - msSinceLastFlush);
        if (msUntilFlush == 0) {
          return null;
        }
        return cmdQueue.poll(msUntilFlush, TimeUnit.MILLISECONDS);
      }
      return cmdQueue.take();
    }

    protected void flush(TriggerType triggerType) throws IOException {
      List<SourcedActionRequest> sourcedReqs = null;
      if (onFlushOpt.isPresent()) {
        //This must be done before the list is cleared in the finally block
        sourcedReqs = requests.stream().map(cmd -> cmd.request).collect(Collectors.toList());
      }

      BulkResult bulkResult = null;
      try {
        bulkResult = client.execute(getBulkRequest());
        //TODO - check for any errors and set error variable
      }
      finally {
        requests.clear();
        lastFlushTsMs = System.currentTimeMillis();
      }
      //Callback flush listener
      if (onFlushOpt.isPresent()) {
        onFlushOpt.get().accept(new BulkReport(bulkResult, triggerType, sourcedReqs));
      }
    }

    protected Bulk getBulkRequest() {
      Bulk.Builder bulkReqBuilder = new Bulk.Builder();
      for (WriterCommand cmd: requests) {
        bulkReqBuilder.addAction(cmd.request.action);
      }
      return bulkReqBuilder.build();
    }

    /**
     *  Responsible for informing main thread of any errors
     *
     *  @return returns true if flush was successful
     */
    protected boolean handleFlushCmd(WriterCommand cmd) {
      //If any errors have previously occurred, fail immediately!!!
      if (error != null) {
        cmd.flushCompletedFuture.completeExceptionally(error);
        return false;
      }
      try {
        flush(TriggerType.FLUSH_CMD);
      }
      catch (Exception e) {
        cmd.flushCompletedFuture.completeExceptionally(e);
        return false;
      }
      cmd.flushCompletedFuture.complete(null);
      return true;
    }

    protected void handleAddCmd(WriterCommand cmd) throws IOException {
      requests.add(cmd);
      if (requests.size() >= config.flushMaxActions) {
        flush(TriggerType.MAX_ACTIONS);
      }
    }

    @Override
    public void run() {
      lastFlushTsMs = System.currentTimeMillis();
      while (true) try {
        WriterCommand cmd = poll();
        if (cmd == null) {
          flush(TriggerType.MAX_INTERVAL);
        } else if (cmd.type.equals(WriterCommandType.ADD_ACTION)) {
          handleAddCmd(cmd);
        } else if (cmd.type.equals(WriterCommandType.FLUSH)) {
          if (!handleFlushCmd(cmd)) {
            logger.error("Elasticsearch writer thread shutting down after error");
            return;
          }
        } else {
          throw new IllegalStateException("Unknown cmd type: " + cmd.type);
        }
      } catch (InterruptedException e) {
        logger.info("Elasticsearch writer thread received shutdown");
        return;
      } catch (Exception e) {
        logger.error("Error writing to Elasticsearch", e);
        error = e;
      }
    }
  }

}
