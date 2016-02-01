/*
 * Copyright 2014-2016 Quantiply Corporation. All rights reserved.
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

        map.put("rico.es.http.host", "foo.com");
        map.put("rico.es.index.prefix", "Slow_svc");
        map.put("rico.es.index.date.zone", "Etc/UTC");
        map.put("rico.es.index.date.format", ".yyyy");
        map.put("rico.es.metadata.source", "key_doc_id");
        map.put("rico.es.doc.type", "slow_svc_type");

        MapConfig config = new MapConfig(map);
        assertFalse(ESPushTaskConfig.isStreamConfig(config));
        ESPushTaskConfig.ESIndexSpec esConfig = ESPushTaskConfig.getDefaultConfig(config);
        assertNotNull(esConfig);

        assertEquals(ESPushTaskConfig.MetadataSrc.KEY_DOC_ID, esConfig.metadataSrc);
        assertEquals("slow_svc", esConfig.indexNamePrefix);
        assertEquals(".yyyy", esConfig.indexNameDateFormat.get());
        assertEquals("Etc/UTC", esConfig.indexNameDateZone.getId());
        assertEquals("slow_svc_type", esConfig.docType);
        assertFalse(esConfig.defaultVersionType.isPresent());
    }

    @Test
    public void testStreamConfig() throws Exception {
        Map<String, String> map = new HashMap<String, String>();

        map.put("rico.es.streams", "server_stats,rep_latency");
        map.put("rico.es.index.date.zone", "Etc/UTC");
        map.put("rico.es.index.date.format", ".yyyy");
        map.put("rico.es.metadata.source", "key_avro");

        map.put("rico.es.stream.server_stats.index.prefix", "db_server_stats_index");
        map.put("rico.es.stream.server_stats.doc.type", "db_server_stats_type");
        map.put("rico.es.stream.server_stats.metadata.source", "embedded");
        map.put("rico.es.stream.server_stats.index.date.format", ".yyyy-MM");
        map.put("rico.es.stream.server_stats.index.date.zone", "America/New_York");
        map.put("rico.es.stream.server_stats.version.type.default", "external_gte");

        map.put("rico.es.stream.rep_latency.index.prefix", "db_rep_latency_index");
        map.put("rico.es.stream.rep_latency.doc.type", "db_rep_latency_type");

        MapConfig config = new MapConfig(map);
        assertTrue(ESPushTaskConfig.isStreamConfig(config));
        Map<String, ESPushTaskConfig.ESIndexSpec> streamMap = ESPushTaskConfig.getStreamMap(config);
        ESPushTaskConfig.ESIndexSpec serverStatsConfig = streamMap.get("server_stats");
        ESPushTaskConfig.ESIndexSpec repLatencyConfig = streamMap.get("rep_latency");
        assertNotNull(serverStatsConfig);
        assertNotNull(repLatencyConfig);

        assertEquals(ESPushTaskConfig.MetadataSrc.EMBEDDED, serverStatsConfig.metadataSrc);
        assertEquals("db_server_stats_index", serverStatsConfig.indexNamePrefix);
        assertEquals(".yyyy-MM", serverStatsConfig.indexNameDateFormat.get());
        assertEquals("America/New_York", serverStatsConfig.indexNameDateZone.getId());
        assertEquals("db_server_stats_type", serverStatsConfig.docType);
        assertTrue(serverStatsConfig.defaultVersionType.isPresent());
        assertEquals(com.quantiply.rico.elasticsearch.VersionType.EXTERNAL_GTE, serverStatsConfig.defaultVersionType.get());

        assertEquals(ESPushTaskConfig.MetadataSrc.KEY_AVRO, repLatencyConfig.metadataSrc);
        assertEquals("db_rep_latency_index", repLatencyConfig.indexNamePrefix);
        assertEquals(".yyyy", repLatencyConfig.indexNameDateFormat.get());
        assertEquals("Etc/UTC", repLatencyConfig.indexNameDateZone.getId());
        assertEquals("db_rep_latency_type", repLatencyConfig.docType);
        assertFalse(repLatencyConfig.defaultVersionType.isPresent());
    }

}