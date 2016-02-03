package com.quantiply.samza.task;

import com.quantiply.elasticsearch.HTTPBulkLoader;
import com.quantiply.rico.elasticsearch.Action;
import com.quantiply.rico.elasticsearch.ActionRequestKey;
import com.quantiply.rico.elasticsearch.VersionType;
import com.quantiply.samza.serde.AvroSerde;
import com.quantiply.samza.serde.JsonSerde;
import org.apache.samza.Partition;
import org.apache.samza.config.MapConfig;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStreamPartition;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ESPushTaskTest {

    @Test
    public void testDefaultDocIdWithKeyConfig() throws Exception {
        Map<String, String> map = new HashMap<String, String>();
        map.put("rico.es.index.prefix", "test");
        map.put("rico.es.index.date.zone", "Etc/UTC");
        map.put("rico.es.index.date.format", ".yyyy");
        map.put("rico.es.metadata.source", "key_doc_id");
        map.put("rico.es.doc.type", "test_type");
        MapConfig config = new MapConfig(map);
        ESPushTaskConfig.ESIndexSpec esConfig = ESPushTaskConfig.getDefaultConfig(config);

        ESPushTask task = new ESPushTask();
        SystemStreamPartition ssp = new SystemStreamPartition("fake", "fake", new Partition(0));
        IncomingMessageEnvelope in = new IncomingMessageEnvelope(ssp, "1234", null, "".getBytes(StandardCharsets.UTF_8));
        long tsNowMs = 1453952662L;
        OutgoingMessageEnvelope out = task.getSimpleOutMsg(in, esConfig, Optional.of(tsNowMs));
        HTTPBulkLoader.ActionRequest req = (HTTPBulkLoader.ActionRequest) out.getMessage();
        assertEquals("fake-0-1234", req.key.getId().toString());
        assertEquals(Action.INDEX, req.key.getAction());
        assertEquals(tsNowMs, req.key.getPartitionTsUnixMs().longValue());
        assertNull("Do not default event time", req.key.getEventTsUnixMs());
        assertNull("Version not set", req.key.getVersion());
        assertNull("Version type not set", req.key.getVersionType());
    }

    @Test
    public void testDefaultDocIdWithAvroKeyConfig() throws Exception {
        Map<String, String> map = new HashMap<String, String>();
        map.put("rico.es.index.prefix", "test");
        map.put("rico.es.index.date.zone", "Etc/UTC");
        map.put("rico.es.index.date.format", ".yyyy");
        map.put("rico.es.metadata.source", "key_avro");
        map.put("rico.es.doc.type", "test_type");
        MapConfig config = new MapConfig(map);
        ESPushTaskConfig.ESIndexSpec esConfig = ESPushTaskConfig.getDefaultConfig(config);

        ESPushTask task = new ESPushTask();
        SystemStreamPartition ssp = new SystemStreamPartition("fake", "fake", new Partition(0));
        ActionRequestKey inKey = ActionRequestKey.newBuilder()
            .setAction(Action.INSERT)
            .setEventTsUnixMs(3L)
            .setPartitionTsUnixMs(4L)
            .setVersionType(VersionType.INTERNAL)
            .setVersion(5L)
            .build();
        task.avroSerde = mock(AvroSerde.class);
        when(task.avroSerde.fromBytes(null)).thenReturn(inKey);
        IncomingMessageEnvelope in = new IncomingMessageEnvelope(ssp, "1234", null, "".getBytes(StandardCharsets.UTF_8));
        OutgoingMessageEnvelope out = task.getAvroKeyOutMsg(in, esConfig);
        HTTPBulkLoader.ActionRequest req = (HTTPBulkLoader.ActionRequest) out.getMessage();
        assertEquals("fake-0-1234", req.key.getId().toString());
        assertEquals(Action.INSERT, req.key.getAction());
        assertEquals(4L, req.key.getPartitionTsUnixMs().longValue());
        assertEquals(3L, req.key.getEventTsUnixMs().longValue());
        assertEquals(VersionType.INTERNAL, req.key.getVersionType());
        assertEquals(5L, req.key.getVersion().longValue());
    }

    @Test
    public void testDefaultDocIdWithJsonKeyConfig() throws Exception {
        Map<String, String> map = new HashMap<String, String>();
        map.put("rico.es.index.prefix", "test");
        map.put("rico.es.index.date.zone", "Etc/UTC");
        map.put("rico.es.index.date.format", ".yyyy");
        map.put("rico.es.metadata.source", "key_json");
        map.put("rico.es.doc.type", "test_type");
        MapConfig config = new MapConfig(map);
        ESPushTaskConfig.ESIndexSpec esConfig = ESPushTaskConfig.getDefaultConfig(config);

        ESPushTask task = new ESPushTask();
        SystemStreamPartition ssp = new SystemStreamPartition("fake", "fake", new Partition(0));
        String jsonStr = "{\"action\":\"INSERT\",\"id\":null,\"version\":5,\"partition_ts_unix_ms\":4,\"event_ts_unix_ms\":3,\"version_type\":\"INTERNAL\"}";
        IncomingMessageEnvelope in = new IncomingMessageEnvelope(ssp, "1234", jsonStr.getBytes(StandardCharsets.UTF_8), "".getBytes(StandardCharsets.UTF_8));
        OutgoingMessageEnvelope out = task.getJsonKeyOutMsg(in, esConfig);
        HTTPBulkLoader.ActionRequest req = (HTTPBulkLoader.ActionRequest) out.getMessage();
        assertEquals("fake-0-1234", req.key.getId().toString());
        assertEquals(Action.INSERT, req.key.getAction());
        assertEquals(4L, req.key.getPartitionTsUnixMs().longValue());
        assertEquals(3L, req.key.getEventTsUnixMs().longValue());
        assertEquals(VersionType.INTERNAL, req.key.getVersionType());
        assertEquals(5L, req.key.getVersion().longValue());
    }

    @Test
    public void testDefaultDocIdWithEmbeddedConfig() throws Exception {
        Map<String, String> map = new HashMap<String, String>();
        map.put("rico.es.index.prefix", "test");
        map.put("rico.es.index.date.zone", "Etc/UTC");
        map.put("rico.es.index.date.format", ".yyyy");
        map.put("rico.es.metadata.source", "embedded");
        map.put("rico.es.doc.type", "test_type");
        MapConfig config = new MapConfig(map);
        ESPushTaskConfig.ESIndexSpec esConfig = ESPushTaskConfig.getDefaultConfig(config);

        ESPushTask task = new ESPushTask();
        SystemStreamPartition ssp = new SystemStreamPartition("fake", "fake", new Partition(0));
        IncomingMessageEnvelope in = new IncomingMessageEnvelope(ssp, "1234", null, null);
        task.jsonSerde = mock(JsonSerde.class);
        when(task.jsonSerde.fromBytes(null)).thenReturn(new HashMap<String, Object>());
        long tsNowMs = 1453952662L;
        OutgoingMessageEnvelope out = task.getEmbeddedOutMsg(in, esConfig, Optional.of(tsNowMs));
        HTTPBulkLoader.ActionRequest req = (HTTPBulkLoader.ActionRequest) out.getMessage();
        assertEquals("fake-0-1234", req.key.getId().toString());
        assertEquals(Action.INDEX, req.key.getAction());
        assertEquals(tsNowMs, req.key.getPartitionTsUnixMs().longValue());
        assertNull("Do not default event time", req.key.getEventTsUnixMs());
        assertNull("Version not set", req.key.getVersion());
        assertNull("Version type not set", req.key.getVersionType());
    }
}