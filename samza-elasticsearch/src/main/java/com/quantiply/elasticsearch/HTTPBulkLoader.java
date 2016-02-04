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

import com.google.gson.Gson;
import com.quantiply.rico.elasticsearch.ActionRequestKey;
import io.searchbox.action.BulkableAction;
import io.searchbox.client.JestClient;
import io.searchbox.core.*;
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
    public final String name;
    public final int flushMaxActions;
    public final Optional<Integer> flushMaxIntervalMs;

    public Config(String name, int flushMaxActions, Optional<Integer> flushMaxIntervalMs) {
      this.name = name;
      this.flushMaxActions = flushMaxActions;
      this.flushMaxIntervalMs = flushMaxIntervalMs;
    }
  }

  public enum TriggerType { MAX_ACTIONS, MAX_INTERVAL, FLUSH_CMD }

  public static class BulkReport {
    public final BulkResult bulkResult;
    public final TriggerType triggerType;
    public final long esWaitMs;
    public final List<SourcedActionRequest> requests;

    public BulkReport(BulkResult bulkResult, TriggerType triggerType, long esWaitMs, List<SourcedActionRequest> requests) {
      this.bulkResult = bulkResult;
      this.triggerType = triggerType;
      this.esWaitMs = esWaitMs;
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

  protected enum WriterCommandType { ADD_ACTION, FLUSH }

  protected static class WriterCommand {

    public static WriterCommand getAddCmd(SourcedActionRequest req) {
      return new WriterCommand(WriterCommandType.ADD_ACTION, req, null);
    }

    public static WriterCommand getFlushCmd() {
      return new WriterCommand(WriterCommandType.FLUSH, null, new CompletableFuture<>());
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
  protected Future<Void> writerFuture = null;
  protected Logger logger = LoggerFactory.getLogger(new Object() {}.getClass().getEnclosingClass());

  /**
   * Elasticsearch HTTP Bulk Loader
   *
   * Methods in this class run in the client's thread
   *
   * JEST client's lifecycle is manage externally (i.e. must be closed elsewhere)
   * so that it may be shared by multiple instances
   *
   * Error handling:
   *   - connection/protocol errors are handled here and considered fatal
   *   - API errors are not checked here. Clients can check them in the onFlush callback and throw exception if fatal
   *   - No internal retry support - restart the process to retry
   */
  public HTTPBulkLoader(Config config, JestClient client, Optional<Consumer<BulkReport>> onFlushOpt) {
    this.writerCmdQueue = new ArrayBlockingQueue<>(config.flushMaxActions);
    final String name = config.name;
    this.writerExecSvc = Executors.newFixedThreadPool(1, r -> new Thread(r, name + " Elasticsearch Writer"));
    this.writer = new Writer(config, client, writerCmdQueue, onFlushOpt);
  }

  /**
   * Converts request to JEST API and passes it to writer thread
   *
   * May block if internal buffer is full
   *
   * Error contract: will throw an Exception if a fatal errors occur in the writer thread
   */
  public void addAction(String source, ActionRequest req) throws Throwable {
//    if (logger.isTraceEnabled()) {
//      logger.trace(String.format("Add action: key %s, index %s/%s, doc %s", req.key, req.index, req.docType, req.document));
//    }
    BulkableAction<DocumentResult> action = convertToJestAction(req);
    WriterCommand addCmd = WriterCommand.getAddCmd(new SourcedActionRequest(source, req, action));
    sendCmd(addCmd);
  }

  /**
   * Issue flush request to writer thread and block until complete
   *
   * Error contract: will throw an Exception if a fatal errors occur in the writer thread
   */
  public void flush() throws Throwable {
    WriterCommand flushCmd = WriterCommand.getFlushCmd();
    sendCmd(flushCmd);
    try {
      //Wait on flush to complete - may block if writer is dead so we must periodically check
      boolean waiting = true;
      while (waiting) {
        try {
          flushCmd.flushCompletedFuture.get(100, TimeUnit.MILLISECONDS);
          waiting = false;
        }
        catch (TimeoutException e) {
          checkWriter();
        }
      }
    }
    catch (InterruptedException e) {
      /* If the main Samza thread is interrupted, it's likely a shutdown command
        Try for a clean shutdown by waiting a little longer on the flush
       */
      try {
        flushCmd.flushCompletedFuture.get(SHUTDOWN_WAIT_MS, TimeUnit.MILLISECONDS);
      } catch (Exception retryEx) {
        throw new IOException("Error trying to flush to Elasticsearch on shutdown", e);
      }
    }
    catch (ExecutionException e) {
      throw e.getCause();
    }
  }

  /**
   * Start writer thread
   */
  public void start() {
    writerFuture = writerExecSvc.submit(writer);
  }

  /**
   * Signal writer thread to shutdown
   */
  public void stop() {
    writerExecSvc.shutdownNow();
    try {
      writerExecSvc.awaitTermination(1, TimeUnit.MINUTES);
    } catch (InterruptedException e) {
      logger.info("Interrupted waiting for Elasticsearch writer shutdown");
    }
  }

  protected BulkableAction<DocumentResult> convertToJestAction(ActionRequest req) {
    BulkableAction<DocumentResult> action;
    switch (req.key.getAction()) {
      case INDEX:
        action = getIndexAction(req);
        break;
      case UPDATE:
        action = getUpdateAction(req);
        break;
      case DELETE:
        action = getDeleteAction(req);
        break;
      default:
        throw new IllegalStateException("Unknown action: " + req.key.getAction());
    }
    return action;
  }

  private BulkableAction<DocumentResult> getIndexAction(ActionRequest req) {
    Index.Builder builder = new Index.Builder(req.document)
        .id(req.key.getId().toString())
        .index(req.index)
        .type(req.docType);
    if (req.key.getVersionType() != null) {
      builder.setParameter(Parameters.VERSION_TYPE, req.key.getVersionType().toString().toLowerCase());
    }
    if (req.key.getVersion() != null) {
      builder.setParameter(Parameters.VERSION, req.key.getVersion());
    }
    return builder.build();
  }

  private BulkableAction<DocumentResult> getUpdateAction(ActionRequest req) {
    Update.Builder builder = new Update.Builder(req.document)
        .id(req.key.getId().toString())
        .index(req.index)
        .type(req.docType);
    if (req.key.getVersionType() != null) {
      builder.setParameter(Parameters.VERSION_TYPE, req.key.getVersionType().toString().toLowerCase());
    }
    if (req.key.getVersion() != null) {
      builder.setParameter(Parameters.VERSION, req.key.getVersion());
    }
    return builder.build();
  }

  private BulkableAction<DocumentResult> getDeleteAction(ActionRequest req) {
    Delete.Builder builder = new Delete.Builder(req.document)
        .id(req.key.getId().toString())
        .index(req.index)
        .type(req.docType);
    if (req.key.getVersionType() != null) {
      builder.setParameter(Parameters.VERSION_TYPE, req.key.getVersionType().toString().toLowerCase());
    }
    if (req.key.getVersion() != null) {
      builder.setParameter(Parameters.VERSION, req.key.getVersion());
    }
    return builder.build();
  }

  protected void checkWriter() throws ExecutionException, InterruptedException {
    if (writerFuture.isDone() || writerFuture.isCancelled()) {
      logger.error("Elasticsearch writer has died");
      writerFuture.get(); //We expect this to throw an exception
      throw new IllegalStateException("Elasticsearch writer has died");
    }
    else {
      logger.trace("Timeout waiting on writer. Writer is still alive. Waiting some more...");
    }
  }

  protected void sendCmd(WriterCommand cmd) throws Throwable {
    try {
      //May block if queue is full so we must periodically check that writer is alive
      while (!writerCmdQueue.offer(cmd, 100, TimeUnit.MILLISECONDS)) {
        checkWriter();
      }
    }
    catch (ExecutionException e) {
      logger.error("Elasticsearch writer died", e.getCause());
      throw e.getCause();
    }
    catch (InterruptedException firstEx) {
      /* If the main Samza thread is interrupted, it's likely a shutdown command
        Try for a clean shutdown by waiting a little longer to enqueue the message
       */
      try {
        if (!writerCmdQueue.offer(cmd, SHUTDOWN_WAIT_MS, TimeUnit.MILLISECONDS)) {
          throw new IOException("Timed out trying to pass message to Elasticsearch writer on shutdown");
        }
      }
      catch (InterruptedException e) {
        throw new IOException("Interrupted passing message to Elasticsearch writer", e);
      }
    }
  }

  /**
   *
   * Writer thread callable - handles all communication with Elasticsearch
   *
   * Error contract: callable finishes on fatal error with exception. On the next
   * operation (addAction or flush) the client thread will detect the problem
   * and throw exception.
   */
  protected class Writer implements Callable<Void> {
    protected final Config config;
    protected final JestClient client;
    protected final Optional<Consumer<BulkReport>> onFlushOpt;
    protected final BlockingQueue<WriterCommand> cmdQueue;
    protected long lastFlushTsMs;
    protected final List<WriterCommand> requests;
    protected Logger logger = LoggerFactory.getLogger(new Object() {}.getClass().getEnclosingClass());

    public Writer(Config config, JestClient client, BlockingQueue<WriterCommand> cmdQueue, Optional<Consumer<BulkReport>> onFlushOpt) {
      this.config = config;
      this.cmdQueue = cmdQueue;
      this.client = client;
      this.onFlushOpt = onFlushOpt;
      this.requests = new ArrayList<>(config.flushMaxActions);
    }

    @Override
    public Void call() throws Exception {
      logger.info("ES writer started");
      try {
        doCall();
      }
      catch (Exception e) {
        logger.error("ES writer dying...");
        throw e;
      }
      logger.info("ES writer is ending");
      return null;
    }

    public void doCall() throws Exception {
      lastFlushTsMs = System.currentTimeMillis();
      while (true) {
        try {
          WriterCommand cmd = poll();
          if (cmd == null) {
            flush(TriggerType.MAX_INTERVAL);
          }
          else if (cmd.type.equals(WriterCommandType.ADD_ACTION)) {
            handleAddCmd(cmd);
          }
          else if (cmd.type.equals(WriterCommandType.FLUSH)) {
            handleFlushCmd(cmd);
          }
          else {
            throw new IllegalStateException("Unknown cmd type: " + cmd.type);
          }
        }
        catch (InterruptedException e) {
          logger.debug("Elasticsearch writer thread shutting down by request");
          return;
        }
        catch (Exception e) {
          logger.error("Error writing to Elasticsearch", e);
          throw e;
        }
      }
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
      if (requests.size() == 0) {
        if (logger.isTraceEnabled()) {
          logger.trace("No records to flush for " + triggerType);
        }
        lastFlushTsMs = System.currentTimeMillis();
        return;
      }

      List<SourcedActionRequest> sourcedReqs = null;
      if (onFlushOpt.isPresent()) {
        //This must be done before the list is cleared in the finally block
        sourcedReqs = requests.stream().map(cmd -> cmd.request).collect(Collectors.toList());
      }

      if (logger.isTraceEnabled()) {
        logger.trace(String.format("Flushing %s actions", requests.size()));
      }
      BulkResult bulkResult = null;
      long esWaitMs = 0;
      try {
        long esStartMs = System.currentTimeMillis();
        Bulk bulkRequest = getBulkRequest();
        if (logger.isTraceEnabled()) {
          String bulkStr = bulkRequest.getData(new Gson());
          logger.trace(bulkStr);
        }
        bulkResult = client.execute(bulkRequest);
        esWaitMs = System.currentTimeMillis() - esStartMs;
      } finally {
        requests.clear();
        lastFlushTsMs = System.currentTimeMillis();
      }
      //Callback flush listener
      if (onFlushOpt.isPresent()) {
        onFlushOpt.get().accept(new BulkReport(bulkResult, triggerType, esWaitMs, sourcedReqs));
      }
    }

    protected Bulk getBulkRequest() {
      Bulk.Builder bulkReqBuilder = new Bulk.Builder();
      for (WriterCommand cmd : requests) {
        bulkReqBuilder.addAction(cmd.request.action);
      }
      return bulkReqBuilder.build();
    }

    /**
     * Informs main thread of any errors via Future and by dying
     */
    protected void handleFlushCmd(WriterCommand cmd) throws Exception {
      logger.trace("Received flush cmd");
      try {
        flush(TriggerType.FLUSH_CMD);
        cmd.flushCompletedFuture.complete(null);
      }
      catch (Exception e) {
        cmd.flushCompletedFuture.completeExceptionally(e);
        throw e;
      }
    }

    protected void handleAddCmd(WriterCommand cmd) throws IOException {
      requests.add(cmd);
//      if (logger.isTraceEnabled()) {
//        logger.trace(String.format("Received add: source %s, action %s, count %s",
//                cmd.request.source, cmd.request.action.getBulkMethodName(), requests.size()));
//      }
      if (requests.size() >= config.flushMaxActions) {
        flush(TriggerType.MAX_ACTIONS);
      }
    }

  }
}
