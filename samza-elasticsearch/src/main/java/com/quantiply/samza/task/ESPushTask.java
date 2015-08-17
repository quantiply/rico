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
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;

/**
 Samza task for pushing to Elasticsearch

 - Requires the Elasticsearch system to be called "es"
 - Requires byte serdes for message keys and values

 */
public class ESPushTask extends BaseTask {
    private SystemStream esStream;
    private AvroSerde avroSerde;
    private JsonSerde jsonSerde;

    @Override
    protected void _init(Config config, TaskContext context, MetricAdaptor metricAdaptor) throws Exception {
        jsonSerde = new JsonSerdeFactory().getSerde("json", config);
        if (getErrorHandler().dropOnError()) {
            logger.warn("Task is configured to drop messages on error");
        }
        boolean isStreamConfig = ESPushTaskConfig.isStreamConfig(config);
        if (isStreamConfig) {
            ESPushTaskConfig.getStreamMap(config).forEach((stream, esIndexSpec) -> {
                registerHandler(stream, getHandler(config, esIndexSpec));
            });
        }
        else {
            registerDefaultHandler(getHandler(config, ESPushTaskConfig.getDefaultConfig(config)));
        }
    }

    private SystemStream getESSystemStream(ESPushTaskConfig.ESIndexSpec spec, Optional<Long> tsNowMsOpt) {
        long tsNowMs = tsNowMsOpt.orElse(System.currentTimeMillis());
        ZonedDateTime dateTime = Instant.ofEpochMilli(tsNowMs).atZone(spec.indexNameDateZone);
        String dateStr = dateTime.format(DateTimeFormatter.ofPattern(spec.indexNameDateFormat));
        return new SystemStream(ESPushTaskConfig.CFS_ES_SYSTEM_NAME, String.format("%s%s/%s", spec.indexNamePrefix, dateStr, spec.docType));
    }

    private Process getHandler(Config config, ESPushTaskConfig.ESIndexSpec esIndexSpec) {
        if (avroSerde == null && esIndexSpec.metadataSrc.equals(ESPushTaskConfig.MetadataSrc.KEY_AVRO)) {
            //Requires additional config for schema registry
            avroSerde = new AvroSerdeFactory().getSerde("avro", config);
        }
        BiFunction<IncomingMessageEnvelope, ESPushTaskConfig.ESIndexSpec, OutgoingMessageEnvelope> msgExtractor = getOutMsgExtractor(esIndexSpec);
        return (envelope, collector, coordinator) -> {
            handleMsg(envelope, collector, coordinator, esIndexSpec, msgExtractor);
        };
    }

    private void handleMsg(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator, ESPushTaskConfig.ESIndexSpec spec, BiFunction<IncomingMessageEnvelope, ESPushTaskConfig.ESIndexSpec, OutgoingMessageEnvelope> msgExtractor) {
        collector.send(msgExtractor.apply(envelope, spec));
    }

    private BiFunction<IncomingMessageEnvelope, ESPushTaskConfig.ESIndexSpec, OutgoingMessageEnvelope> getOutMsgExtractor(ESPushTaskConfig.ESIndexSpec spec) {
        BiFunction<IncomingMessageEnvelope, ESPushTaskConfig.ESIndexSpec, OutgoingMessageEnvelope> func = null;
        switch (spec.metadataSrc) {
            case KEY_DOC_ID:
                func = this::getSimpleOutMsg;
                break;
            case KEY_AVRO:
                func = this::getKeyOutMsg;
                break;
            case EMBEDDED:
                func = this::getEmbeddedOutMsg;
                break;
        }
        return func;
    }

    private OutgoingMessageEnvelope getSimpleOutMsg(IncomingMessageEnvelope envelope, ESPushTaskConfig.ESIndexSpec spec) {
        SystemStream stream = getESSystemStream(spec, Optional.empty());
        //Message key is used for the document id
        String id = null;
        if (envelope.getKey() != null) {
            id = new String((byte [])envelope.getKey(), StandardCharsets.UTF_8);
        }
        if (logger.isDebugEnabled()) {
            logger.debug(String.format("Sending document to ES index %s with id %s", stream.getStream(), id));
        }
        return new OutgoingMessageEnvelope(stream, null, id, envelope.getMessage());
    }

    private OutgoingMessageEnvelope getKeyOutMsg(IncomingMessageEnvelope envelope, ESPushTaskConfig.ESIndexSpec spec) {
        IndexRequestKey key = (IndexRequestKey) avroSerde.fromBytes((byte[]) envelope.getKey());
        SystemStream stream = getESSystemStream(spec, Optional.of(key.getTimestampUnixMs()));
        setDefaultVersionType(key, spec);
        if (logger.isDebugEnabled()) {
            logger.debug(String.format("Sending document to ES index %s with metadata %s", stream.getStream(), key));
        }
        return new OutgoingMessageEnvelope(stream, null, key, envelope.getMessage());
    }

    private OutgoingMessageEnvelope getEmbeddedOutMsg(IncomingMessageEnvelope envelope, ESPushTaskConfig.ESIndexSpec spec) {
        Map<String, Object> document = (Map<String, Object>)jsonSerde.fromBytes((byte[]) envelope.getMessage());
        IndexRequestKey.Builder keyBuilder = IndexRequestKey.newBuilder();
        if (document.containsKey("_id") && document.get("_id") instanceof String) {
            keyBuilder.setId((String) document.get("_id"));
            document.remove("_id");
        }
        if (document.containsKey("_version") && document.get("_version") instanceof Number) {
            keyBuilder.setVersion(((Number) document.get("_version")).longValue());
            document.remove("_version");
        }
        if (document.containsKey("_version_type") && document.get("_version_type") instanceof String) {
            keyBuilder.setVersionType(VersionType.valueOf(((String) document.get("_version_type")).toUpperCase()));
            document.remove("_version_type");
        }
        if (document.containsKey("_timestamp") && document.get("_timestamp") instanceof String) {
            keyBuilder.setTimestamp((String) document.get("_timestamp"));
            document.remove("_timestamp");
        }
        if (document.containsKey("@timestamp") && document.get("@timestamp") instanceof String) {
            keyBuilder.setTimestampUnixMs(((Number) document.get("@timestamp")).longValue());
            document.remove("@timestamp");
        }
        if (document.containsKey("_ttl") && document.get("_ttl") instanceof Number) {
            keyBuilder.setTtl(((Number) document.get("_ttl")).longValue());
            document.remove("_ttl");
        }
        IndexRequestKey key = keyBuilder.build();
        SystemStream stream = getESSystemStream(spec, Optional.of(key.getTimestampUnixMs()));
        setDefaultVersionType(key, spec);
        if (logger.isDebugEnabled()) {
            logger.debug(String.format("Sending document to ES index %s with metadata %s", stream.getStream(), key));
        }
        return new OutgoingMessageEnvelope(stream, null, key, document);
    }

    private void setDefaultVersionType(IndexRequestKey key, ESPushTaskConfig.ESIndexSpec spec) {
        if (key.getVersionType() == null && spec.defaultVersionType.isPresent()) {
            key.setVersionType(spec.defaultVersionType.get());
        }
    }
}
