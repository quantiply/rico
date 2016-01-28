package com.quantiply.samza.task;

import com.quantiply.rico.elasticsearch.Action;
import com.quantiply.rico.elasticsearch.ActionRequestKey;
import com.quantiply.rico.elasticsearch.VersionType;
import com.quantiply.samza.serde.AvroSerde;
import org.apache.samza.Partition;
import org.apache.samza.config.MapConfig;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemStreamPartition;
import org.junit.Test;

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
        IncomingMessageEnvelope in = new IncomingMessageEnvelope(ssp, "1234", null, null);
        long tsNowMs = 1453952662L;
        ActionRequestKey requestKey = task.getSimpleOutMsg(in, esConfig, Optional.of(tsNowMs));
        assertEquals("fake-0-1234", requestKey.getId().toString());
        assertEquals(Action.INDEX, requestKey.getAction());
        assertEquals(tsNowMs, requestKey.getPartitionTsUnixMs().longValue());
        assertEquals(tsNowMs, requestKey.getEventTsUnixMs().longValue());
        assertNull("Version not set", requestKey.getVersion());
        assertNull("Version type not set", requestKey.getVersionType());
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
            .setAction(Action.DELETE)
            .setEventTsUnixMs(3L)
            .setPartitionTsUnixMs(4L)
            .setVersionType(VersionType.INTERNAL)
            .setVersion(5L)
            .build();
        task.avroSerde = mock(AvroSerde.class);
        when(task.avroSerde.fromBytes(null)).thenReturn(inKey);
        IncomingMessageEnvelope in = new IncomingMessageEnvelope(ssp, "1234", null, null);
        ActionRequestKey requestKey = task.getAvroKeyOutMsg(in, esConfig);
        assertEquals("fake-0-1234", requestKey.getId().toString());
        assertEquals(Action.DELETE, requestKey.getAction());
        assertEquals(4L, requestKey.getPartitionTsUnixMs().longValue());
        assertEquals(3L, requestKey.getEventTsUnixMs().longValue());
        assertEquals(VersionType.INTERNAL, requestKey.getVersionType());
        assertEquals(5L, requestKey.getVersion().longValue());
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
        IncomingMessageEnvelope in = new IncomingMessageEnvelope(ssp, "1234", null, "{}");
        long tsNowMs = 1453952662L;
        ActionRequestKey requestKey = task.getEmbeddedOutMsg(in, esConfig, Optional.of(tsNowMs));
        assertEquals("fake-0-1234", requestKey.getId().toString());
        assertEquals(Action.INDEX, requestKey.getAction());
        assertEquals(tsNowMs, requestKey.getPartitionTsUnixMs().longValue());
        assertEquals(tsNowMs, requestKey.getEventTsUnixMs().longValue());
        assertNull("Version not set", requestKey.getVersion());
        assertNull("Version type not set", requestKey.getVersionType());
    }
}