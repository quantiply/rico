package com.quantiply.samza.system.elasticsearch;

import com.google.gson.Gson;
import com.quantiply.elasticsearch.HTTPBulkLoader;
import com.quantiply.rico.elasticsearch.Action;
import com.quantiply.rico.elasticsearch.ActionRequestKey;
import io.searchbox.client.JestClient;
import io.searchbox.core.BulkResult;
import io.searchbox.core.Index;
import org.apache.samza.metrics.MetricsRegistryMap;
import org.apache.samza.util.SystemClock;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ElasticsearchSystemProducerTest {
  private static final String SYSTEM_NAME = "es";
  private static final HTTPBulkLoaderFactory BULK_LOADER_FACTORY = mock(HTTPBulkLoaderFactory.class);
  private static final JestClient CLIENT = mock(JestClient.class);
  private ElasticsearchSystemProducer producer;
  private ElasticsearchSystemProducerMetrics metrics;
  private ElasticsearchSystemProducer.FlushListener flushListener;

  @Before
  public void setUp() throws Exception {
    metrics = new ElasticsearchSystemProducerMetrics("es", new MetricsRegistryMap());
    producer = new ElasticsearchSystemProducer(SYSTEM_NAME,
        BULK_LOADER_FACTORY,
        CLIENT,
        ElasticsearchSystemFactory.MSG_TO_ACTION,
        metrics);
    flushListener = new ElasticsearchSystemProducer.FlushListener(metrics, SYSTEM_NAME, mock(SystemClock.class));
  }

  @Test
  public void testMetrics() throws Exception {
    long esWaitMs = 200;
    long receivedMs = 100;
    long eventTsMs = 50;
    long tsNow = 300;
    List<HTTPBulkLoader.SourcedActionRequest> requests = new ArrayList<>();
    requests.add(getIndexRequest(100, 50));
    requests.add(getIndexRequest(100, 50));
    requests.add(getIndexRequest(100, 50));
    requests.add(getIndexRequest(100, 50));
    requests.add(getIndexRequest(100, 50));

    List<BulkResult.BulkResultItem> items = new ArrayList<>();
    items.add(getItemIndexInsert());
    items.add(getItemIndexUpdate());
    items.add(getItemIndexConflict());
    items.add(getItemUpdate());
    items.add(getItemDelete());

    BulkResult bulkResult = mock(BulkResult.class);
    when(bulkResult.getItems()).thenReturn(items);
    HTTPBulkLoader.BulkReport report = new HTTPBulkLoader.BulkReport(bulkResult, HTTPBulkLoader.TriggerType.MAX_ACTIONS, esWaitMs, requests);

    when(flushListener.clock.currentTimeMillis()).thenReturn(tsNow);
    flushListener.accept(report);
    assertEquals(1, metrics.bulkSendSuccess.getCount());
    assertEquals(5.0, metrics.bulkSendBatchSize.getSnapshot().getMean(), 0.0001);
    assertEquals(esWaitMs, metrics.bulkSendWaitMs.getSnapshot().getMean(), 0.0001);
    assertEquals(1, metrics.inserts.getCount());
    assertEquals(2, metrics.updates.getCount());
    assertEquals(1, metrics.conflicts.getCount());
    assertEquals(1, metrics.deletes.getCount());
    assertEquals(0, metrics.triggerFlushCmd.getCount());
    assertEquals(1, metrics.triggerMaxActions.getCount());
    assertEquals(0, metrics.triggerMaxInterval.getCount());
    assertEquals(tsNow - eventTsMs, metrics.lagFromOriginMs.getSnapshot().getMean(), 0.0001);
    assertEquals(tsNow - receivedMs, metrics.lagFromReceiveMs.getSnapshot().getMean(), 0.0001);
  }

  private BulkResult.BulkResultItem getItemIndexInsert() {
    return new BulkResult(new Gson()).new BulkResultItem("index", "test", "test", "test", 201, "");
  }

  private BulkResult.BulkResultItem getItemIndexUpdate() {
    return new BulkResult(new Gson()).new BulkResultItem("index", "test", "test", "test", 200, "");
  }

  private BulkResult.BulkResultItem getItemIndexConflict() {
    return new BulkResult(new Gson()).new BulkResultItem("index", "test", "test", "test", 409, "");
  }

  private BulkResult.BulkResultItem getItemUpdate() {
    return new BulkResult(new Gson()).new BulkResultItem("update", "test", "test", "test", 200, "");
  }

  private BulkResult.BulkResultItem getItemDelete() {
    return new BulkResult(new Gson()).new BulkResultItem("delete", "test", "test", "test", 200, "");
  }


  private HTTPBulkLoader.SourcedActionRequest getIndexRequest(long receivedTsMs, long eventTsMs) {
    ActionRequestKey key = ActionRequestKey.newBuilder().setAction(Action.INDEX)
        .setId("foo")
        .setEventTsUnixMs(eventTsMs)
        .setPartitionTsUnixMs(eventTsMs)
        .build();
    HTTPBulkLoader.ActionRequest req = new HTTPBulkLoader.ActionRequest(key, "testindex", "testtype", receivedTsMs, "{}");
    return new HTTPBulkLoader.SourcedActionRequest("testSource", req, mock(Index.class));
  }

}