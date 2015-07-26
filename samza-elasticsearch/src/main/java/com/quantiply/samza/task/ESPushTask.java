/*
 * Copyright 2014-2015 Quantiply Corporation. All rights reserved.
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

import com.quantiply.rico.elasticsearch.IndexRequestKey;
import com.quantiply.rico.elasticsearch.VersionType;
import com.quantiply.samza.MetricAdaptor;
import com.quantiply.samza.serde.AvroSerde;
import com.quantiply.samza.serde.AvroSerdeFactory;
import org.apache.samza.config.Config;
import org.apache.samza.config.ConfigException;
import org.apache.samza.serializers.JsonSerde;
import org.apache.samza.serializers.JsonSerdeFactory;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

/**
 Samza task for pushing to Elasticsearch

 Currently, index names are chosen by the time of import
 not the date of data.  The assumption is that these will
 be close to each other and that aliases will be used
 for querying that cover time periods that may overlap.

 - Requires the Elasticsearch system to be called "es"
 - Requires byte serdes for message keys and values

 */
public class ESPushTask extends BaseTask {
    private enum MetadataSrc { NONE, KEY, EMBEDDED }
    private final static String CFS_ES_SYSTEM_NAME = "es";
    private final static String CFG_ES_INDEX_PREFIX = "rico.es.index.prefix";
    private final static String CFG_ES_INDEX_DATE_FORMAT = "rico.es.index.date.format";
    private final static String CFG_ES_INDEX_DATE_ZONE = "rico.es.index.date.zone";
    private final static String CFG_ES_DOC_TYPE = "rico.es.doc.type";
    private final static String CFS_ES_DOC_METADATA_SRC = "rico.es.doc.metadata.source";
    private final static long INDEX_NAME_CACHE_DURATION_MS = 60 * 1000L;
    private final static HashSet<String> METADATA_SRC_OPTIONS = Arrays.stream(MetadataSrc.values()).map(v -> v.toString().toLowerCase()).collect(Collectors.toCollection(HashSet::new));
    private static long updatedMs = 0L;
    private SystemStream esStream;
    private String indexNamePrefix;
    private String docType;
    private String dateFormat;
    private String dateZone;
    private MetadataSrc metadataSrc;
    private AvroSerde avroSerde;
    private JsonSerde jsonSerde;
    private BiFunction<IncomingMessageEnvelope, SystemStream, OutgoingMessageEnvelope> outMsgExtractor;

    @Override
    protected void _init(Config config, TaskContext context, MetricAdaptor metricAdaptor) throws Exception {
        parseESConfig();
        registerDefaultHandler(this::processMsg);
        if (getErrorHandler().dropOnError()) {
            logger.warn("Task is configured to drop messages on error");
        }
        if (metadataSrc == MetadataSrc.KEY) {
            avroSerde = new AvroSerdeFactory().getSerde("avro", config);
        }
        if (metadataSrc == MetadataSrc.EMBEDDED) {
            jsonSerde = new JsonSerdeFactory().getSerde("json", config);
        }
        outMsgExtractor = getOutMsgExtractor();
    }

    private <R> void parseESConfig() {
        indexNamePrefix = config.get(CFG_ES_INDEX_PREFIX);
        if (indexNamePrefix == null) {
            throw new ConfigException("Missing config property for Elasticearch index prefix: " + CFG_ES_INDEX_PREFIX);
        }
        dateFormat = config.get(CFG_ES_INDEX_DATE_FORMAT, "");
        if (dateFormat == null) {
            throw new ConfigException("Missing config property Elasticearch index date format: " + CFG_ES_INDEX_DATE_FORMAT);
        }
        dateZone = config.get(CFG_ES_INDEX_DATE_ZONE, ZoneId.systemDefault().toString());
        if (dateZone == null) {
            throw new ConfigException("Missing config property Elasticearch index time zone: " + CFG_ES_INDEX_DATE_ZONE);
        }
        docType = config.get(CFG_ES_DOC_TYPE);
        if (docType == null) {
            throw new ConfigException("Missing config property for Elasticearch index doc type: " + CFG_ES_DOC_TYPE);
        }
        String metadataSrcStr = config.get(CFS_ES_DOC_METADATA_SRC, "none").toLowerCase();
        if (!METADATA_SRC_OPTIONS.contains(metadataSrcStr)) {
            throw new ConfigException(String.format("Bad value for metadata src param: %s.  Options are: %s",
                    CFS_ES_DOC_METADATA_SRC,
                    String.join(",", METADATA_SRC_OPTIONS)));
        }
        metadataSrc = MetadataSrc.valueOf(metadataSrcStr.toUpperCase());
    }

    private SystemStream getESSystemStream(long tsNowMs) {
        if (esStream == null || (tsNowMs - updatedMs) > INDEX_NAME_CACHE_DURATION_MS) {
            esStream = calcESSystemStream(tsNowMs);
            updatedMs = tsNowMs;
        }
        return esStream;
    }

    private SystemStream calcESSystemStream(long tsNowMs) {
        ZonedDateTime dateTime = Instant.ofEpochMilli(tsNowMs).atZone(ZoneId.of(dateZone));
        String dateStr = dateTime.format(DateTimeFormatter.ofPattern(dateFormat));
        return new SystemStream(CFS_ES_SYSTEM_NAME, String.format("%s%s/%s", indexNamePrefix, dateStr, docType));
    }

    private void processMsg(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) throws Exception {
        long tsNowMs = System.currentTimeMillis();
        SystemStream stream = getESSystemStream(tsNowMs);
        collector.send(outMsgExtractor.apply(envelope, stream));
    }

    private BiFunction<IncomingMessageEnvelope, SystemStream, OutgoingMessageEnvelope> getOutMsgExtractor() {
        BiFunction<IncomingMessageEnvelope, SystemStream, OutgoingMessageEnvelope> func = null;
        switch (metadataSrc) {
            case NONE:
                func = this::getSimpleOutMsg;
                break;
            case KEY:
                func = this::getKeyOutMsg;
                break;
            case EMBEDDED:
                func = this::getEmbeddedOutMsg;
                break;
        }
        return func;
    }

    private OutgoingMessageEnvelope getSimpleOutMsg(IncomingMessageEnvelope envelope, SystemStream stream) {
        //Message key is used for the document id
        String id = null;
        if (envelope.getKey() !=  null) {
            id = new String((byte [])envelope.getKey(), StandardCharsets.UTF_8);
        }
        if (logger.isDebugEnabled()) {
            logger.debug(String.format("Sending document to ES index %s with id %s", stream.getStream(), id));
        }
        return new OutgoingMessageEnvelope(stream, null, id, envelope.getMessage());
    }

    private OutgoingMessageEnvelope getKeyOutMsg(IncomingMessageEnvelope envelope, SystemStream stream) {
        IndexRequestKey key = (IndexRequestKey) avroSerde.fromBytes((byte[]) envelope.getKey());
        if (logger.isDebugEnabled()) {
            logger.debug(String.format("Sending document to ES index %s with metadata %s", stream.getStream(), key));
        }
        return new OutgoingMessageEnvelope(stream, null, key, envelope.getMessage());
    }

    private OutgoingMessageEnvelope getEmbeddedOutMsg(IncomingMessageEnvelope envelope, SystemStream stream) {
        Map<String, Object> document = (Map<String, Object>)jsonSerde.fromBytes((byte[])envelope.getMessage());
        IndexRequestKey.Builder keyBuilder = IndexRequestKey.newBuilder().setId((String)document.get("_id"));
        if (document.containsKey("_version") && document.get("_version") instanceof Long) {
            keyBuilder.setVersion((Long)document.get("_version"));
            document.remove("_version");
        }
        if (document.containsKey("_version_type") && document.get("_version_type") instanceof String) {
            keyBuilder.setVersionType(VersionType.valueOf((String) document.get("_version_type")));
            document.remove("_version_type");
        }
        if (document.containsKey("_timestamp") && document.get("_timestamp") instanceof String) {
            keyBuilder.setTimestamp((String) document.get("_timestamp"));
            document.remove("_timestamp");
        }
        if (document.containsKey("_ttl") && document.get("_ttl") instanceof Long) {
            keyBuilder.setVersion((Long) document.get("_ttl"));
            document.remove("_ttl");
        }
        IndexRequestKey key = keyBuilder.build();
        if (logger.isDebugEnabled()) {
            logger.debug(String.format("Sending document to ES index %s with metadata %s", stream.getStream(), key));
        }
        return new OutgoingMessageEnvelope(stream, null, key, document);
    }

}
