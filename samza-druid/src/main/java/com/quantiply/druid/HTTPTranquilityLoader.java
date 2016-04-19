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
package com.quantiply.druid;

import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.samza.serializers.JsonSerde;
import org.apache.samza.serializers.JsonSerdeFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class HTTPTranquilityLoader {

  public static class Config {
    public final String name;
    public final String tranquilityServerUrl;
    public final int flushMaxRecords;
    public final Optional<Integer> flushMaxIntervalMs;

    public Config(String name, String tranquilityServerUrl, int flushMaxRecords, Optional<Integer> flushMaxIntervalMs) {
      this.name = name;
      this.tranquilityServerUrl = tranquilityServerUrl;
      this.flushMaxRecords = flushMaxRecords;
      this.flushMaxIntervalMs = flushMaxIntervalMs;
    }
  }

  public enum TriggerType { MAX_RECORDS, MAX_INTERVAL, FLUSH_CMD }

  public static class Response {
    public final int received;
    public final int sent;

    public Response(int received, int sent) {
      this.received = received;
      this.sent = sent;
    }
  }

  public static class BulkReport {
    public final Response response;
    public final TriggerType triggerType;
    public final long waitMs;
    public final List<SourcedIndexRequest> requests;

    public BulkReport(Response response, TriggerType triggerType, long waitMs, List<SourcedIndexRequest> requests) {
      this.response = response;
      this.triggerType = triggerType;
      this.waitMs = waitMs;
      this.requests = requests;
    }
  }

  public static class IndexRequest {
    public final Optional<Long> eventTsMs;
    public final long receivedTsMs;
    public final byte[] record;

    public IndexRequest(Optional<Long> eventTsMs, long receivedTsMs, byte[] record) {
      this.eventTsMs = eventTsMs;
      this.receivedTsMs = receivedTsMs;
      this.record = record;
    }
  }

  public static class SourcedIndexRequest {
    public final IndexRequest request;
    public final String source;

    public SourcedIndexRequest(String source, IndexRequest request) {
      this.request = request;
      this.source = source;
    }
  }

  protected enum WriterCommandType { ADD_RECORD, FLUSH }

  protected static class WriterCommand {

    public static WriterCommand getAddCmd(SourcedIndexRequest req) {
      return new WriterCommand(WriterCommandType.ADD_RECORD, req, null);
    }

    public static WriterCommand getFlushCmd() {
      return new WriterCommand(WriterCommandType.FLUSH, null, new CompletableFuture<>());
    }

    public WriterCommand(WriterCommandType type, SourcedIndexRequest request, CompletableFuture<Void> flushCompletedFuture) {
      this.type = type;
      this.request = request;
      this.flushCompletedFuture = flushCompletedFuture;
    }

    public final WriterCommandType type;
    public final SourcedIndexRequest request;
    public final CompletableFuture<Void> flushCompletedFuture;
  }

  protected static final int SHUTDOWN_WAIT_MS = 100;
  protected final String dataSource;
  protected final Writer writer;
  protected final ArrayBlockingQueue<WriterCommand> writerCmdQueue;
  protected final ExecutorService writerExecSvc;
  protected Future<Void> writerFuture = null;
  protected Logger logger = LoggerFactory.getLogger(new Object() {}.getClass().getEnclosingClass());

  /**
   * Druid Tranquility HTTP Loader
   *
   * Methods in this class run in the client's thread
   *
   * Error handling:
   *   - connection/protocol errors are handled here and considered fatal - they are detected on blocking operations
   *      - addAction (when cmd queue is full)
   *      - flush
   *   - API errors are checked here and are also considered fatal
   *   - No internal retry support - restart the process to retry
   */
  public HTTPTranquilityLoader(String dataSource, Config config, Optional<Consumer<BulkReport>> onFlushOpt) {
    this.dataSource = dataSource;
    this.writerCmdQueue = new ArrayBlockingQueue<>(config.flushMaxRecords);
    final String name = config.name;
    this.writerExecSvc = Executors.newFixedThreadPool(1, r -> new Thread(r, name + " Tranquility Writer"));
    this.writer = new Writer(config, writerCmdQueue, onFlushOpt);
  }

  /**
   * Pass request to writer thread
   *
   * May block if internal buffer is full
   *
   * Error contract: will throw an Exception if a fatal errors occur in the writer thread
   */
  public void addAction(String source, IndexRequest req) throws Throwable {
    if (logger.isTraceEnabled()) {
      logger.trace(String.format("Add index request: dataSource %s, time %s, record %s", dataSource, req.eventTsMs, new String(req.record)));
    }
    WriterCommand addCmd = WriterCommand.getAddCmd(new SourcedIndexRequest(source, req));
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
        throw new IOException("Error trying to flush to Tranquility server on shutdown", e);
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
      logger.info("Interrupted waiting for Tranquility writer shutdown");
    }
  }

  protected void checkWriter() throws ExecutionException, InterruptedException {
    if (writerFuture.isDone() || writerFuture.isCancelled()) {
      logger.error("Tranquility writer has died");
      writerFuture.get(); //We expect this to throw an exception
      throw new IllegalStateException("Tranquility writer has died");
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
      logger.error("Tranquility writer died", e.getCause());
      throw e.getCause();
    }
    catch (InterruptedException firstEx) {
      /* If the main Samza thread is interrupted, it's likely a shutdown command
        Try for a clean shutdown by waiting a little longer to enqueue the message
       */
      try {
        if (!writerCmdQueue.offer(cmd, SHUTDOWN_WAIT_MS, TimeUnit.MILLISECONDS)) {
          throw new IOException("Timed out trying to pass message to Tranquility writer on shutdown");
        }
      }
      catch (InterruptedException e) {
        throw new IOException("Interrupted passing message to Tranquility writer", e);
      }
    }
  }

  /**
   *
   * Writer thread callable - handles all communication with Tranquility server
   *
   * Error contract: callable finishes on fatal error with exception. On the next
   * blocking operation (addAction with full queue or flush) the client thread will detect the problem
   * and throw exception.
   */
  protected class Writer implements Callable<Void> {
    protected final byte[] newLineBytes = "\n".getBytes(StandardCharsets.UTF_8);
    protected final CloseableHttpClient httpClient;
    protected final Config config;
    protected final Optional<Consumer<BulkReport>> onFlushOpt;
    protected final BlockingQueue<WriterCommand> cmdQueue;
    protected final JsonSerde jsonSerde;
    protected long lastFlushTsMs;
    protected final List<WriterCommand> requests;
    protected Logger logger = LoggerFactory.getLogger(new Object() {}.getClass().getEnclosingClass());

    public Writer(Config config, BlockingQueue<WriterCommand> cmdQueue, Optional<Consumer<BulkReport>> onFlushOpt) {
      this.config = config;
      this.cmdQueue = cmdQueue;
      this.onFlushOpt = onFlushOpt;
      this.requests = new ArrayList<>(config.flushMaxRecords);
      httpClient = HttpClients.createDefault();
      jsonSerde = new JsonSerdeFactory().getSerde("json", null);
    }

    @Override
    public Void call() throws Exception {
      logger.info("Tranquility writer started");
      try {
        doCall();
      }
      catch (Exception e) {
        logger.error("Tranquility writer dying...");
        throw e;
      }
      logger.info("Tranquility writer is ending");
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
          else if (cmd.type.equals(WriterCommandType.ADD_RECORD)) {
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
          logger.debug("Tranquility writer thread shutting down by request");
          return;
        }
        catch (Exception e) {
          logger.error("Error writing to Tranquility server", e);
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

      List<SourcedIndexRequest> sourcedReqs = null;
      if (onFlushOpt.isPresent()) {
        //This must be done before the list is cleared in the finally block
        sourcedReqs = requests.stream().map(cmd -> cmd.request).collect(Collectors.toList());
      }

      if (logger.isTraceEnabled()) {
        logger.trace(String.format("Flushing %s records", requests.size()));
      }
      long waitMs = 0;
      Response response;
      try {
        long startMs = System.currentTimeMillis();
        response = sendToServer();
        waitMs = System.currentTimeMillis() - startMs;
      }
      finally {
        requests.clear();
        lastFlushTsMs = System.currentTimeMillis();
      }
      //Callback flush listener on success
      if (onFlushOpt.isPresent()) {
        onFlushOpt.get().accept(new BulkReport(response, triggerType, waitMs, sourcedReqs));
      }
    }

    /**
     *
     * Tranquility protocol: https://github.com/druid-io/tranquility/blob/master/docs/server.md
     */
    protected Response sendToServer() throws IOException {
      assert requests.size() > 0;

      HttpPost httpPost = new HttpPost(config.tranquilityServerUrl + dataSource);
      ByteArrayEntity entity = new ByteArrayEntity(getBody());
      httpPost.setEntity(entity);
      httpPost.setHeader("Content-type", "application/json");

      try (CloseableHttpResponse response = httpClient.execute(httpPost)) {
        int statusCode = response.getStatusLine().getStatusCode();
        HttpEntity respEntity = response.getEntity();
        if (statusCode != 200) {
          String bodyStr = respEntity == null ? null : EntityUtils.toString(respEntity);
          throw new IOException(String.format("Tranquility server error. Status code %s: %s", statusCode, bodyStr));
        }
        return parseResponse(respEntity);
      }
    }

    protected Response parseResponse(HttpEntity respEntity) throws IOException {
      try {
        Map<String, Map<String, Integer>> reply = (Map<String, Map<String, Integer>>) jsonSerde.fromBytes(EntityUtils.toByteArray(respEntity));
        Map<String, Integer> result = reply.get("result");
        assert result != null;
        return new Response(result.get("received"), result.get("sent"));
      }
      catch (Exception e) {
        throw new IOException("Error parsing response from Tranquility server", e);
      }
    }

    protected byte[] getBody() {
      int size4Records = requests.stream().mapToInt(cmd -> cmd.request.request.record.length).sum();
      int size = size4Records + newLineBytes.length*requests.size();

      ByteBuffer buffer = ByteBuffer.wrap(new byte[size]);
      for (WriterCommand cmd : requests) {
        buffer.put(cmd.request.request.record);
        buffer.put(newLineBytes);
      }
      return buffer.array();
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
//        logger.trace(String.format("Received add: source %s, count %s",
//                cmd.request.source, requests.size()));
//      }
      if (requests.size() >= config.flushMaxRecords) {
        flush(TriggerType.MAX_RECORDS);
      }
    }

  }
}
