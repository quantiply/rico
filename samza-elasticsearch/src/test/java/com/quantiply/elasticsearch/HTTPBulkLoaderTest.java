package com.quantiply.elasticsearch;

import com.google.gson.Gson;
import com.quantiply.rico.elasticsearch.Action;
import com.quantiply.rico.elasticsearch.ActionRequestKey;
import com.quantiply.rico.elasticsearch.VersionType;
import io.searchbox.action.BulkableAction;
import io.searchbox.client.JestClient;
import io.searchbox.core.DocumentResult;
import io.searchbox.params.Parameters;
import org.junit.Test;

import java.io.IOException;
import java.security.InvalidParameterException;
import java.util.Optional;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import static com.jayway.awaitility.Awaitility.await;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HTTPBulkLoaderTest {

  @Test
  public void testConvertToJestActionIndex() throws Exception {
    HTTPBulkLoader loader = getBulkLoader();
    ActionRequestKey key = ActionRequestKey.newBuilder()
        .setAction(Action.INDEX)
        .setId("myId")
        .build();
    BulkableAction<DocumentResult> action = loader.convertToJestAction(new HTTPBulkLoader.ActionRequest(key, "fakeindex", "faketype", 100L, "{}"));
    assertEquals("index", action.getBulkMethodName());
    assertEquals("myId", action.getId());
    assertEquals("fakeindex", action.getIndex());
    assertEquals("faketype", action.getType());
    assertEquals("{}", action.getData(new Gson()));
    assertEquals(0, action.getParameter(Parameters.VERSION).size());
    assertEquals(0, action.getParameter(Parameters.VERSION_TYPE).size());

    ActionRequestKey keyWithVersion = ActionRequestKey.newBuilder()
        .setAction(Action.INDEX)
        .setId("myId")
        .setVersion(123L)
        .setVersionType(VersionType.EXTERNAL)
        .build();
    BulkableAction<DocumentResult> actionWithVersion = loader.convertToJestAction(new HTTPBulkLoader.ActionRequest(keyWithVersion, "fakeindex", "faketype", 100L, "{}"));
    assertEquals("external", actionWithVersion.getParameter(Parameters.VERSION_TYPE).toArray()[0]);
    assertEquals(123L, actionWithVersion.getParameter(Parameters.VERSION).toArray()[0]);
  }

  @Test
  public void testConvertToJestActionUpdate() throws Exception {
    HTTPBulkLoader loader = getBulkLoader();
    ActionRequestKey key = ActionRequestKey.newBuilder()
        .setAction(Action.UPDATE)
        .setId("myId")
        .setVersion(123L)
        .setVersionType(VersionType.EXTERNAL)
        .build();
    BulkableAction<DocumentResult> action = loader.convertToJestAction(new HTTPBulkLoader.ActionRequest(key, "fakeindex", "faketype", 100L, "{}"));
    assertEquals("update", action.getBulkMethodName());
    assertEquals("myId", action.getId());
    assertEquals("fakeindex", action.getIndex());
    assertEquals("faketype", action.getType());
    assertEquals("external", action.getParameter(Parameters.VERSION_TYPE).toArray()[0]);
    assertEquals(123L, action.getParameter(Parameters.VERSION).toArray()[0]);
    assertEquals("{}", action.getData(new Gson()));
  }

  @Test
  public void testConvertToJestActionDelete() throws Exception {
    HTTPBulkLoader loader = getBulkLoader();
    ActionRequestKey key = ActionRequestKey.newBuilder()
        .setAction(Action.DELETE)
        .setId("myId")
        .setVersion(123L)
        .setVersionType(VersionType.EXTERNAL)
        .build();
    BulkableAction<DocumentResult> action = loader.convertToJestAction(new HTTPBulkLoader.ActionRequest(key, "fakeindex", "faketype", 100L, null));
    assertEquals("delete", action.getBulkMethodName());
    assertEquals("myId", action.getId());
    assertEquals("fakeindex", action.getIndex());
    assertEquals("faketype", action.getType());
    assertEquals("external", action.getParameter(Parameters.VERSION_TYPE).toArray()[0]);
    assertEquals(123L, action.getParameter(Parameters.VERSION).toArray()[0]);
    assertEquals(null, action.getData(new Gson()));
  }

  @Test
  public void testDeadWriterDetectionOnAdd() throws Throwable {
    HTTPBulkLoader loader = getBulkLoader();
    loader.writerFuture = mock(Future.class);
    when(loader.writerFuture.isDone()).thenReturn(true);
    when(loader.writerFuture.get()).thenThrow(new RuntimeException("TEST"));

    HTTPBulkLoader.ActionRequest req = getRequest();
    loader.addAction("test", req); //this should not fail b/c queue is not yet full

    //This should fail b/c queue is full
    assertThatThrownBy(() -> loader.addAction("test", req)).isInstanceOf(RuntimeException.class)
            .hasMessageContaining("TEST");
  }

  @Test
  public void testDeadWriterDetectionOnFlushCmdSend() throws Throwable {
    HTTPBulkLoader loader = getBulkLoader();
    loader.writerFuture = mock(Future.class);
    when(loader.writerFuture.isDone()).thenReturn(true);
    when(loader.writerFuture.get()).thenThrow(new RuntimeException("TEST"));

    HTTPBulkLoader.ActionRequest req = getRequest();
    loader.addAction("test", req); //this should not fail b/c queue is not yet full

    //This should fail b/c queue is full
    assertThatThrownBy(loader::flush).isInstanceOf(RuntimeException.class)
            .hasMessageContaining("TEST");
  }

  @Test
  public void testDeadWriterDetectionOnFlushWait() throws Throwable {
    HTTPBulkLoader loader = getBulkLoader();
    loader.writerFuture = mock(Future.class);
    when(loader.writerFuture.isDone()).thenReturn(true);
    when(loader.writerFuture.get()).thenThrow(new RuntimeException("TEST"));

    //This on wait timeout - not because queue is full
    assertThatThrownBy(loader::flush).isInstanceOf(RuntimeException.class)
            .hasMessageContaining("TEST");
  }

  @Test
  public void testWriterMaxActions() throws Throwable {
    int maxActions = 10;
    HTTPBulkLoader.Config config = new HTTPBulkLoader.Config("Test", maxActions, Optional.empty());
    JestClient client = mock(JestClient.class);
    AtomicInteger numFlushes = new AtomicInteger(0);
    HTTPBulkLoader loader = new HTTPBulkLoader(config, client, Optional.of(bulkReport -> numFlushes.incrementAndGet()));

    loader.start();
    IntStream.range(0, 10).forEach(i -> {
      try {
        loader.addAction("test", getRequest());
      } catch (Throwable throwable) {}
    });
    //Missing one action until we hit max - no flushes should have happened yet
    assertEquals(0, numFlushes.get());
    loader.addAction("test", getRequest());
    //Next add should trigger flush
    await().atMost(150, TimeUnit.MILLISECONDS).until(() -> numFlushes.get() == 1);
    loader.stop();
  }

  @Test
  public void testWriterMaxInterval() throws Throwable {
    int maxIntervalMs = 150;
    HTTPBulkLoader.Config config = new HTTPBulkLoader.Config("Test", 1, Optional.of(maxIntervalMs));
    JestClient client = mock(JestClient.class);
    AtomicInteger numFlushes = new AtomicInteger(0);
    HTTPBulkLoader loader = new HTTPBulkLoader(config, client, Optional.of(bulkReport -> numFlushes.incrementAndGet()));

    loader.start();
    Thread.sleep(maxIntervalMs + 10);
    //Will not flush on max interval if no docs were added
    assertEquals(0, numFlushes.get());
    loader.addAction("test", getRequest());
    //Now that there's a record it should have flushed
    await().atMost(maxIntervalMs, TimeUnit.MILLISECONDS).until(() -> numFlushes.get() == 1);
    loader.stop();
  }

  @Test
  public void testWriterMaxIntervalFlushResets() throws Throwable {
    int maxIntervalMs = 1000;
    HTTPBulkLoader.Config config = new HTTPBulkLoader.Config("Test", 10, Optional.of(maxIntervalMs));
    JestClient client = mock(JestClient.class);
    AtomicInteger numFlushes = new AtomicInteger(0);
    HTTPBulkLoader loader = new HTTPBulkLoader(config, client, Optional.of(bulkReport -> numFlushes.incrementAndGet()));

    loader.start();
    loader.addAction("test", getRequest());
    assertEquals(0, numFlushes.get());
    Thread.sleep(maxIntervalMs/2 + 1);
    assertEquals(0, numFlushes.get());
    //Manually flushing - should reset interval timer
    loader.flush();
    loader.addAction("test", getRequest());
    assertEquals(1, numFlushes.get());
    Thread.sleep(maxIntervalMs/2 + 1);
    //Original time interval has expired - there should still only be a single flush (the manual one)
    assertEquals(1, numFlushes.get());
    //Now that there's a record it should have flushed
    await().atMost(maxIntervalMs, TimeUnit.MILLISECONDS).until(() -> numFlushes.get() == 2);
    loader.stop();
  }

  private HTTPBulkLoader.ActionRequest getRequest() {
    long tsNow = 12345L;
    ActionRequestKey key = ActionRequestKey.newBuilder()
            .setAction(Action.INDEX)
            .setId("blah")
            .build();
    return new HTTPBulkLoader.ActionRequest(key, "testindex", "testtype", tsNow, "{}");
  }

  private HTTPBulkLoader getBulkLoader() {
    HTTPBulkLoader.Config config = new HTTPBulkLoader.Config("Test", 1, Optional.empty());
    JestClient client = mock(JestClient.class);
    return new HTTPBulkLoader(config, client, Optional.empty());
  }

}