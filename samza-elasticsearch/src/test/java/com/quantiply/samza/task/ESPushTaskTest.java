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
import static org.assertj.core.api.Assertions.*;

import java.nio.charset.StandardCharsets;
import java.security.InvalidParameterException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ESPushTaskTest {

    protected static final SystemStreamPartition ssp = new SystemStreamPartition("fake", "fake", new Partition(0));

    @Test
    public void testDefaultDocIdWithKeyConfig() throws Exception {
        ESPushTaskConfig.ESIndexSpec esConfig = getEsIndexSpec("key_doc_id", true);
        ESPushTask task = getEsPushTask();
        long tsNowMs = 1453952662L;
        OutgoingMessageEnvelope out = task.getSimpleOutMsg(getInMsg(""), esConfig, Optional.of(tsNowMs));
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
        ESPushTaskConfig.ESIndexSpec esConfig = getEsIndexSpec("key_avro", true);
        ESPushTask task = getEsPushTask();
        ActionRequestKey inKey = ActionRequestKey.newBuilder()
            .setEventTsUnixMs(3L)
            .setPartitionTsUnixMs(4L)
            .setVersionType(VersionType.EXTERNAL)
            .setVersion(5L)
            .build();
        when(task.avroSerde.fromBytes(null)).thenReturn(inKey);
        OutgoingMessageEnvelope out = task.getAvroKeyOutMsg(getInMsg(""), esConfig);
        HTTPBulkLoader.ActionRequest req = (HTTPBulkLoader.ActionRequest) out.getMessage();
        assertEquals("fake-0-1234", req.key.getId().toString());
        assertEquals(Action.INDEX, req.key.getAction());
        assertEquals(4L, req.key.getPartitionTsUnixMs().longValue());
        assertEquals(3L, req.key.getEventTsUnixMs().longValue());
        assertEquals(VersionType.EXTERNAL, req.key.getVersionType());
        assertEquals(5L, req.key.getVersion().longValue());
    }

    @Test
    public void testWritesWithPartitionedIndexes() throws Exception {
        ESPushTaskConfig.ESIndexSpec esConfig = getEsIndexSpec("key_avro", true);
        ESPushTask task = getEsPushTask();

        ActionRequestKey inKeyMissingId = ActionRequestKey.newBuilder()
            .setAction(Action.UPDATE)
            .setPartitionTsUnixMs(99L)
            .build();
        when(task.avroSerde.fromBytes(null)).thenReturn(inKeyMissingId);

        assertThatThrownBy(() -> { task.getAvroKeyOutMsg(getInMsg(""), esConfig); }).isInstanceOf(InvalidParameterException.class)
            .hasMessageContaining("Document id is required");

        ActionRequestKey inKeyMissingTs = ActionRequestKey.newBuilder()
            .setAction(Action.DELETE)
            .setId("foo")
            .build();
        when(task.avroSerde.fromBytes(null)).thenReturn(inKeyMissingTs);

        assertThatThrownBy(() -> { task.getAvroKeyOutMsg(getInMsg(""), esConfig); }).isInstanceOf(InvalidParameterException.class)
            .hasMessageContaining("Partition timestamp is required");

        ActionRequestKey inKey = ActionRequestKey.newBuilder()
            .setAction(Action.DELETE)
            .setId("foo")
            .setPartitionTsUnixMs(99L)
            .build();
        when(task.avroSerde.fromBytes(null)).thenReturn(inKey);

        OutgoingMessageEnvelope out = task.getAvroKeyOutMsg(getInMsg(""), esConfig);
        HTTPBulkLoader.ActionRequest req = (HTTPBulkLoader.ActionRequest) out.getMessage();
        assertEquals("foo", req.key.getId().toString());
        assertEquals(Action.DELETE, req.key.getAction());
        assertEquals(99L, req.key.getPartitionTsUnixMs().longValue());
        assertNull("Do not default event time", req.key.getEventTsUnixMs());
        assertNull("No version set", req.key.getVersion());
        assertNull("No version type set", req.key.getVersionType());
    }

    @Test
    public void testDocMissing() throws Exception {
        ESPushTaskConfig.ESIndexSpec esConfig = getEsIndexSpec("key_avro", false);
        ESPushTask task = getEsPushTask();

        ActionRequestKey inKeyUpdate = ActionRequestKey.newBuilder()
            .setAction(Action.UPDATE)
            .setId("foo")
            .build();
        when(task.avroSerde.fromBytes(null)).thenReturn(inKeyUpdate);

        assertThatThrownBy(() -> task.getAvroKeyOutMsg(getInMsg(null), esConfig)).isInstanceOf(InvalidParameterException.class)
            .hasMessageContaining("Document must be provided");

        ActionRequestKey inKeyIndex = ActionRequestKey.newBuilder()
            .setAction(Action.INDEX)
            .setId("foo")
            .build();
        when(task.avroSerde.fromBytes(null)).thenReturn(inKeyIndex);

        assertThatThrownBy(() -> task.getAvroKeyOutMsg(getInMsg(null), esConfig)).isInstanceOf(InvalidParameterException.class)
            .hasMessageContaining("Document must be provided");

    }

    @Test
    public void testWritesWithToStaticIndex() throws Exception {
        ESPushTaskConfig.ESIndexSpec esConfig = getEsIndexSpec("key_avro", false);
        ESPushTask task = getEsPushTask();

        ActionRequestKey inKeyMissingId = ActionRequestKey.newBuilder()
            .setAction(Action.DELETE)
            .build();
        when(task.avroSerde.fromBytes(null)).thenReturn(inKeyMissingId);
        assertThatThrownBy(() -> task.getAvroKeyOutMsg(getInMsg(""), esConfig)).isInstanceOf(InvalidParameterException.class)
            .hasMessageContaining("Document id is required");

        ActionRequestKey inKey = ActionRequestKey.newBuilder()
            .setAction(Action.UPDATE)
            .setId("blah")
            .build();
        when(task.avroSerde.fromBytes(null)).thenReturn(inKey);

        OutgoingMessageEnvelope out = task.getAvroKeyOutMsg(getInMsg("{}"), esConfig);
        HTTPBulkLoader.ActionRequest req = (HTTPBulkLoader.ActionRequest) out.getMessage();
        assertEquals("blah", req.key.getId().toString());
        assertEquals(Action.UPDATE, req.key.getAction());
        assertNull("Do not default partition time", req.key.getPartitionTsUnixMs());
        assertNull("Do not default event time", req.key.getEventTsUnixMs());
        assertNull("No version set", req.key.getVersion());
        assertNull("No version type set", req.key.getVersionType());
        assertEquals("{\"doc\":{}}", req.document);
    }

    @Test
    public void testDefaultDocIdWithJsonKeyConfig() throws Exception {
        ESPushTaskConfig.ESIndexSpec esConfig = getEsIndexSpec("key_json", true);
        ESPushTask task = getEsPushTask();
        String jsonStr = "{\"action\":\"INDEX\",\"id\":null,\"version\":5,\"partition_ts_unix_ms\":4,\"event_ts_unix_ms\":3,\"version_type\":\"force\"}";
        OutgoingMessageEnvelope out = task.getJsonKeyOutMsg(getInMsg(jsonStr, ""), esConfig);
        HTTPBulkLoader.ActionRequest req = (HTTPBulkLoader.ActionRequest) out.getMessage();
        assertEquals("fake-0-1234", req.key.getId().toString());
        assertEquals(Action.INDEX, req.key.getAction());
        assertEquals(4L, req.key.getPartitionTsUnixMs().longValue());
        assertEquals(3L, req.key.getEventTsUnixMs().longValue());
        assertEquals(VersionType.FORCE, req.key.getVersionType());
        assertEquals(5L, req.key.getVersion().longValue());
    }

    @Test
    public void testDefaultDocIdWithEmbeddedConfig() throws Exception {
        ESPushTaskConfig.ESIndexSpec esConfig = getEsIndexSpec("embedded", true);
        ESPushTask task = getEsPushTask();
        HashMap<String, Object> doc = new HashMap<>();
        when(task.jsonSerde.fromBytes(null)).thenReturn(doc);
        when(task.jsonSerde.toString(doc)).thenReturn("");
        long tsNowMs = 1453952662L;
        OutgoingMessageEnvelope out = task.getEmbeddedOutMsg(getInMsg(null), esConfig, Optional.of(tsNowMs));
        HTTPBulkLoader.ActionRequest req = (HTTPBulkLoader.ActionRequest) out.getMessage();
        assertEquals("fake-0-1234", req.key.getId().toString());
        assertEquals(Action.INDEX, req.key.getAction());
        assertEquals(tsNowMs, req.key.getPartitionTsUnixMs().longValue());
        assertNull("Do not default event time", req.key.getEventTsUnixMs());
        assertNull("Version not set", req.key.getVersion());
        assertNull("Version type not set", req.key.getVersionType());
    }


    private ESPushTask getEsPushTask() {
        ESPushTask task = new ESPushTask();
        task.avroSerde = mock(AvroSerde.class);
        task.jsonSerde = mock(JsonSerde.class);
        return task;
    }

    private ESPushTaskConfig.ESIndexSpec getEsIndexSpec(String src, boolean partitionByTime) {
        Map<String, String> map = new HashMap<>();
        map.put("rico.es.index.prefix", "test");
        if (partitionByTime) {
            map.put("rico.es.index.date.zone", "Etc/UTC");
            map.put("rico.es.index.date.format", ".yyyy");
        }
        map.put("rico.es.metadata.source", src);
        map.put("rico.es.doc.type", "test_type");
        MapConfig config = new MapConfig(map);
        return ESPushTaskConfig.getDefaultConfig(config);
    }

    private IncomingMessageEnvelope getInMsg(String doc) {
        return getInMsg(null, doc);
    }

    private IncomingMessageEnvelope getInMsg(String key, String doc) {
        byte [] keyBytes = null;
        if (key != null) {
            keyBytes = key.getBytes(StandardCharsets.UTF_8);
        }
        byte [] docBytes = null;
        if (doc != null) {
            docBytes = doc.getBytes(StandardCharsets.UTF_8);
        }
        return new IncomingMessageEnvelope(ssp, "1234", keyBytes, docBytes);
    }
}