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
    public void testConfig() throws Exception {
        Map<String, String> map = new HashMap<String, String>();
        map.put("systems.es.index.request.factory", "com.quantiply.samza.elasticsearch.AvroKeyIndexRequestFactory");

        map.put("rico.es.streams", "server_stats,rep_latency");
        map.put("rico.es.index.date.zone", "Etc/UTC");
        map.put("rico.es.index.date.format", ".yyyy");
        map.put("rico.es.doc.metadata.source", "key_avro");

        map.put("rico.es.stream.server_stats.input", "db_server_stats_topic");
        map.put("rico.es.stream.server_stats.index.prefix", "db_server_stats_index");
        map.put("rico.es.stream.server_stats.index.doc.type", "db_server_stats_type");
        map.put("rico.es.stream.server_stats.doc.metadata.source", "embedded");
        map.put("rico.es.stream.server_stats.index.date.format", ".yyyy-MM");

        map.put("rico.es.stream.rep_latency.input", "db_rep_latency_topic");
        map.put("rico.es.stream.rep_latency.index.prefix", "db_rep_latency_index");
        map.put("rico.es.stream.rep_latency.doc.type", "db_rep_latency_type");

        Map<String, ESPushTaskConfig.ESIndexSpec> streamMap = ESPushTaskConfig.getStreamMap(new MapConfig(map));
        ESPushTaskConfig.ESIndexSpec serverStatsConfig = streamMap.get("db_server_stats_topic");
        ESPushTaskConfig.ESIndexSpec repLatencyConfig = streamMap.get("db_rep_latency_topic");
        assertNotNull(serverStatsConfig);
        assertNotNull(repLatencyConfig);

        assertEquals("db_server_stats_topic", serverStatsConfig.input);
        assertEquals(ESPushTaskConfig.MetadataSrc.EMBEDDED, serverStatsConfig.metadataSrc);
        assertEquals("db_server_stats_index", serverStatsConfig.indexNamePrefix);
        assertEquals(".yyyy-MM", serverStatsConfig.indexNameDateFormat);

        assertEquals("db_rep_latency_topic", repLatencyConfig.input);
        assertEquals(ESPushTaskConfig.MetadataSrc.KEY_AVRO, repLatencyConfig.metadataSrc);
        assertEquals("db_rep_latency_index", repLatencyConfig.indexNamePrefix);
        assertEquals(".yyyy", repLatencyConfig.indexNameDateFormat);

    }


}