package com.quantiply.samza.task;

import org.apache.samza.config.ConfigException;
import org.apache.samza.config.MapConfig;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class ESPushTaskConfigTest {

    @Test(expected = ConfigException.class)
    public void testMissingStreamsParam() throws Exception {
        Map<String, String> map = new HashMap<String, String>();
        ESPushTaskConfig.getStreamMap(new MapConfig(map));
    }

    @Test(expected = ConfigException.class)
    public void testEmptyStreamsParam() throws Exception {
        Map<String, String> map = new HashMap<String, String>();
        map.put("rico.es.streams", "");
        ESPushTaskConfig.getStreamMap(new MapConfig(map));
    }

    @Test
    public void testDefaultConfig() throws Exception {
        Map<String, String> map = new HashMap<String, String>();
        map.put("systems.es.index.request.factory", "com.quantiply.samza.elasticsearch.AvroKeyIndexRequestFactory");

        map.put("rico.es.index.prefix", "slow_svc");
        map.put("rico.es.index.date.zone", "Etc/UTC");
        map.put("rico.es.index.date.format", ".yyyy");
        map.put("rico.es.metadata.source", "key_doc_id");
        map.put("rico.es.doc.type", "slow_svc_type");

        MapConfig config = new MapConfig(map);
        assertFalse(ESPushTaskConfig.isStreamConfig(config));
        ESPushTaskConfig.ESIndexSpec esConfig = ESPushTaskConfig.getDefaultConfig(config);
        assertNotNull(esConfig);

        assertEquals("default", esConfig.input);
        assertEquals(ESPushTaskConfig.MetadataSrc.KEY_DOC_ID, esConfig.metadataSrc);
        assertEquals("slow_svc", esConfig.indexNamePrefix);
        assertEquals(".yyyy", esConfig.indexNameDateFormat);
        assertEquals("Etc/UTC", esConfig.indexNameDateZone.getId());
        assertEquals("slow_svc_type", esConfig.docType);
        assertFalse(esConfig.defaultVersionType.isPresent());
    }

    @Test
    public void testStreamConfig() throws Exception {
        Map<String, String> map = new HashMap<String, String>();
        map.put("systems.es.index.request.factory", "com.quantiply.samza.elasticsearch.AvroKeyIndexRequestFactory");

        map.put("rico.es.streams", "server_stats,rep_latency");
        map.put("rico.es.index.date.zone", "Etc/UTC");
        map.put("rico.es.index.date.format", ".yyyy");
        map.put("rico.es.metadata.source", "key_avro");

        map.put("rico.es.stream.server_stats.input", "db_server_stats_topic");
        map.put("rico.es.stream.server_stats.index.prefix", "db_server_stats_index");
        map.put("rico.es.stream.server_stats.doc.type", "db_server_stats_type");
        map.put("rico.es.stream.server_stats.metadata.source", "embedded");
        map.put("rico.es.stream.server_stats.index.date.format", ".yyyy-MM");
        map.put("rico.es.stream.server_stats.index.date.zone", "America/New_York");
        map.put("rico.es.stream.server_stats.version.type.default", "external_gte");

        map.put("rico.es.stream.rep_latency.input", "db_rep_latency_topic");
        map.put("rico.es.stream.rep_latency.index.prefix", "db_rep_latency_index");
        map.put("rico.es.stream.rep_latency.doc.type", "db_rep_latency_type");

        MapConfig config = new MapConfig(map);
        assertTrue(ESPushTaskConfig.isStreamConfig(config));
        Map<String, ESPushTaskConfig.ESIndexSpec> streamMap = ESPushTaskConfig.getStreamMap(config);
        ESPushTaskConfig.ESIndexSpec serverStatsConfig = streamMap.get("db_server_stats_topic");
        ESPushTaskConfig.ESIndexSpec repLatencyConfig = streamMap.get("db_rep_latency_topic");
        assertNotNull(serverStatsConfig);
        assertNotNull(repLatencyConfig);

        assertEquals("db_server_stats_topic", serverStatsConfig.input);
        assertEquals(ESPushTaskConfig.MetadataSrc.EMBEDDED, serverStatsConfig.metadataSrc);
        assertEquals("db_server_stats_index", serverStatsConfig.indexNamePrefix);
        assertEquals(".yyyy-MM", serverStatsConfig.indexNameDateFormat);
        assertEquals("America/New_York", serverStatsConfig.indexNameDateZone.getId());
        assertEquals("db_server_stats_type", serverStatsConfig.docType);
        assertTrue(serverStatsConfig.defaultVersionType.isPresent());
        assertEquals(com.quantiply.rico.elasticsearch.VersionType.EXTERNAL_GTE, serverStatsConfig.defaultVersionType.get());

        assertEquals("db_rep_latency_topic", repLatencyConfig.input);
        assertEquals(ESPushTaskConfig.MetadataSrc.KEY_AVRO, repLatencyConfig.metadataSrc);
        assertEquals("db_rep_latency_index", repLatencyConfig.indexNamePrefix);
        assertEquals(".yyyy", repLatencyConfig.indexNameDateFormat);
        assertEquals("Etc/UTC", repLatencyConfig.indexNameDateZone.getId());
        assertEquals("db_rep_latency_type", repLatencyConfig.docType);
        assertFalse(repLatencyConfig.defaultVersionType.isPresent());
    }

}