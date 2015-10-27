package com.quantiply.samza.task;

import com.quantiply.rico.elasticsearch.IndexRequestKey;
import com.quantiply.samza.serde.AvroSerde;
import com.quantiply.samza.serde.AvroSerdeFactory;
import org.apache.samza.Partition;
import org.apache.samza.config.MapConfig;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStreamPartition;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.samza.util.Clock;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class ESPushTaskTest {

    @Test
    public void testDefaultDocIdWithKeyConfig() throws Exception {
        Map<String, String> map = new HashMap<String, String>();
        map.put("systems.es.index.request.factory", "com.quantiply.samza.elasticsearch.IndexRequestFromKey");
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
        OutgoingMessageEnvelope out = task.getSimpleOutMsg(in, esConfig);
        assertEquals("fake-0-1234", out.getKey().toString());
    }

    @Test
    public void testDefaultDocIdWithAvroKeyConfig() throws Exception {
        Map<String, String> map = new HashMap<String, String>();
        map.put("systems.es.index.request.factory", "com.quantiply.samza.elasticsearch.IndexRequestFromKey");
        map.put("rico.es.index.prefix", "test");
        map.put("rico.es.index.date.zone", "Etc/UTC");
        map.put("rico.es.index.date.format", ".yyyy");
        map.put("rico.es.metadata.source", "key_avro");
        map.put("rico.es.doc.type", "test_type");
        MapConfig config = new MapConfig(map);
        ESPushTaskConfig.ESIndexSpec esConfig = ESPushTaskConfig.getDefaultConfig(config);

        ESPushTask task = new ESPushTask();
        SystemStreamPartition ssp = new SystemStreamPartition("fake", "fake", new Partition(0));
        IndexRequestKey key = IndexRequestKey.newBuilder().build();
        task.avroSerde = mock(AvroSerde.class);
        when(task.avroSerde.fromBytes(null)).thenReturn(key);
        IncomingMessageEnvelope in = new IncomingMessageEnvelope(ssp, "1234", null, null);
        OutgoingMessageEnvelope out = task.getSimpleOutMsg(in, esConfig);
        assertEquals("fake-0-1234", out.getKey().toString());
    }

    @Test
    public void testDefaultDocIdWithEmbeddedConfig() throws Exception {
        Map<String, String> map = new HashMap<String, String>();
        map.put("systems.es.index.request.factory", "com.quantiply.samza.elasticsearch.IndexRequestFromKey");
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
        OutgoingMessageEnvelope out = task.getSimpleOutMsg(in, esConfig);
        assertEquals("fake-0-1234", out.getKey().toString());
    }
}