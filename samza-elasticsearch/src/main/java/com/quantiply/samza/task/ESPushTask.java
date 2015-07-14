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

import com.quantiply.samza.MetricAdaptor;
import org.apache.samza.config.Config;
import org.apache.samza.config.ConfigException;
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

/**
 Samza task for pushing to Elasticsearch

 Currently, index names are chosen by the time of import
 not the date of data.  The assumption is that these will
 be close to each other and that aliases will be used
 for querying that cover time periods that may overlap.

 */
public class ESPushTask extends BaseTask {
    private final static String CFG_ES_INDEX_PREFIX = "rico.es.index.prefix";
    private final static String CFG_ES_INDEX_DATE_FORMAT = "rico.es.index.date.format";
    private final static String CFG_ES_INDEX_DATE_ZONE = "rico.es.index.date.zone";
    private final static String CFG_ES_DOC_TYPE = "rico.es.doc.type";
    private final static long INDEX_NAME_CACHE_DURATION_MS = 60 * 1000L;
    private static long updatedMs = 0L;
    private SystemStream esStream;
    private String indexNamePrefix;
    private String docType;
    private String dateFormat;
    private String dateZone;

    @Override
    protected void _init(Config config, TaskContext context, MetricAdaptor metricAdaptor) throws Exception {
        parseESConfig();
        registerDefaultHandler(this::processMsg);
        if (getErrorHandler().dropOnError()) {
            logger.warn("Task is configured to drop messages on error");
        }
    }

    private void parseESConfig() {
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
        return new SystemStream("es", String.format("%s%s/%s", indexNamePrefix, dateStr, docType));
    }

    private void processMsg(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) throws Exception {
        long tsNowMs = System.currentTimeMillis();
        //Message key is used for the document id
        String id = new String((byte [])envelope.getKey(), StandardCharsets.UTF_8);
        SystemStream stream = getESSystemStream(tsNowMs);
        if (logger.isDebugEnabled()) {
            logger.debug(String.format("Sending document to ES index %s with id %s", stream.getStream(), id));
        }
        collector.send(new OutgoingMessageEnvelope(stream, null, id, envelope.getMessage()));
    }
}
