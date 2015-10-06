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

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.quantiply.samza.ConfigConst;
import com.quantiply.samza.MetricAdaptor;
import com.quantiply.samza.admin.KafkaAdmin;
import com.quantiply.samza.admin.TaskInfo;
import com.quantiply.samza.metrics.EventStreamMetrics;
import com.quantiply.samza.metrics.StreamMetricFactory;
import com.quantiply.samza.metrics.StreamMetricRegistry;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.generic.IndexedRecord;
import org.apache.samza.config.Config;
import org.apache.samza.config.ConfigException;
import org.apache.samza.job.JobRunner;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.task.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public abstract class BaseTask implements InitableTask, StreamTask, ClosableTask {
    protected TaskInfo taskInfo;
    protected Config config;
    protected Logger logger = LoggerFactory.getLogger(new Object(){}.getClass().getEnclosingClass());

    private final Map<String, StreamMsgHandler> handlerMap = new HashMap<>();
    private Optional<StreamMsgHandler> defaultHandler = Optional.empty();
    private MetricAdaptor metricAdaptor;
    private ErrorHandler errorHandler;

    @FunctionalInterface
    public interface Process {
        void apply(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) throws Exception;
    }

    @FunctionalInterface
    public interface ProcessWithMetrics<M> {
        void apply(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator, M customMetrics) throws Exception;
    }

    public class StreamMetrics {
        public final Meter processed;
        public final Meter dropped;

        public StreamMetrics(StreamMetricRegistry registry) {
            processed = registry.meter("processed");
            dropped = registry.meter("dropped");
        }
    }

    private class StreamMsgHandler {
        private Optional<String> name;
        private Process processFunc;
        private StreamMetrics metrics;

        public StreamMsgHandler(Optional<String> name, Process processFunc, StreamMetrics metrics) {
            this.name = name;
            this.processFunc = processFunc;
            this.metrics = metrics;
        }

        public Optional<String> getName() {
            return name;
        }

        public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) throws Exception {
            try {
                processFunc.apply(envelope, collector, coordinator);
                metrics.processed.mark();
            }
            catch (Exception e) {
                logger.error("Exception processing message", e);
                errorHandler.handleException(envelope, e);
                //If control reaches here, the message was dropped
                metrics.dropped.mark();
            }
        }
    }

    public ErrorHandler getErrorHandler() {
        return errorHandler;
    }

    public void handleExpectedError(IncomingMessageEnvelope envelope, Exception e) throws Exception {
        if (logger.isInfoEnabled()) {
            logger.info("Error processing message", e);
        }
        StreamMsgHandler handler = getStreamMsgHandler(envelope);
        errorHandler.handleExpectedError(envelope, e);
        handler.metrics.dropped.mark();
    }

    @Override
    public void init(Config config, TaskContext context) throws Exception {
        this.config = config;
        taskInfo = new TaskInfo(config, context);
        metricAdaptor = new MetricAdaptor(new MetricRegistry(), context.getMetricsRegistry(), ConfigConst.METRICS_GROUP_NAME);
        errorHandler = new ErrorHandler(config, taskInfo);
        errorHandler.start();

        _init(config, context, metricAdaptor);

        validateHandlers(context);
    }

    private void validateHandlers(TaskContext context) {
        if (defaultHandler.isPresent()) {
            return;
        }
        for (SystemStreamPartition ssp: context.getSystemStreamPartitions()) {
            if (!handlerMap.containsKey(ssp.getStream())) {
                throw new ConfigException(
                        String.format("Missing handler for stream: %s. Call registerDefaultHandler() or registerHandler() in _init().", ssp.getStream())
                );
            }
        }
    }

    @Override
    public final void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) throws Exception {
        taskInfo.setMDC(envelope);

        //Dispatch
        StreamMsgHandler handler = getStreamMsgHandler(envelope);
        handler.process(envelope, collector, coordinator);

        taskInfo.clearMDC();
    }

    private StreamMsgHandler getStreamMsgHandler(IncomingMessageEnvelope envelope) {
        StreamMsgHandler handler;
        String streamName = envelope.getSystemStreamPartition().getStream();
        if (handlerMap.containsKey(streamName)) {
            handler = handlerMap.get(streamName);
        }
        else if (defaultHandler.isPresent()) {
            handler = defaultHandler.get();
        }
        else {
            throw new ConfigException("No handler for input stream: " + streamName);
        }
        return handler;
    }

    @Override
    public final void close() throws Exception {
        logger.debug("Shutting down");

        Exception error = null;
        if (errorHandler != null) {
            try {
                errorHandler.stop();
            }
            catch (Exception e) {
                logger.error("Error stopping error handler", e);
                error = e;
            }
        }

        try {
            _close();
        }
        catch (Exception e) {
            logger.error("Error on close", e);
            error = e;
        }
        if (error != null) {
            throw error;
        }
    }

    protected void _close() throws Exception {}

    protected abstract void _init(Config config, TaskContext context, MetricAdaptor metricAdaptor) throws Exception;

    protected void registerDefaultHandler(Process processFunc) {
        defaultHandler = Optional.of(newStreamMsgHandler(Optional.empty(), processFunc));
    }

    protected <M> void registerDefaultHandler(ProcessWithMetrics<M> processFunc, StreamMetricFactory<M> metricFactory) {
        defaultHandler = Optional.of(newStreamMsgHandler(Optional.empty(), processFunc, metricFactory));
    }

    protected void registerHandler(String logicalStreamName, Process processFunc) {
        String streamName = getStreamName(logicalStreamName);
        StreamMsgHandler handler = newStreamMsgHandler(Optional.of(streamName), processFunc);
        handlerMap.put(handler.getName().get(), handler);
    }

    protected <M> void registerHandler(String logicalStreamName, ProcessWithMetrics<M> processFunc, StreamMetricFactory<M> metricFactory) {
        StreamMsgHandler handler = newStreamMsgHandler(Optional.of(logicalStreamName), processFunc, metricFactory);
        handlerMap.put(handler.getName().get(), handler);
    }

    private <M> StreamMsgHandler newStreamMsgHandler(Optional<String> logicalStreamName, ProcessWithMetrics<M> processWithMetrics, StreamMetricFactory<M> metricFactory) {
        Optional<String> streamName = logicalStreamName.map(this::getStreamName);
        M custom = metricFactory.create(new StreamMetricRegistry(getStreamMetricPrefix(streamName), metricAdaptor));
        Process process = (envelope, collector, coordinator) -> processWithMetrics.apply(envelope, collector, coordinator, custom);
        return newStreamMsgHandler(streamName, process);
    }

    private StreamMsgHandler newStreamMsgHandler(Optional<String> streamName, Process process) {
        StreamMetrics metrics = new StreamMetrics(new StreamMetricRegistry(getStreamMetricPrefix(streamName), metricAdaptor));
        return new StreamMsgHandler(streamName, process, metrics);
    }

    private String getStreamMetricPrefix(Optional<String> streamName) {
        String metricName = streamName.map(StreamMetricRegistry::sanitizeStreamNameForMetrics).orElse("default");
        return String.format("streams.%s.", metricName);
    }

    protected String getMessageIdFromSource(IncomingMessageEnvelope envelope) {
        SystemStreamPartition ssp = envelope.getSystemStreamPartition();
        return String.format("%s-%d-%s", ssp.getStream(), ssp.getPartition().getPartitionId(), envelope.getOffset());
    }

    protected void updateLagMetricsForCamusRecord(IndexedRecord msg, long tsNowMs, EventStreamMetrics metrics) {
        Schema.Field headerField = msg.getSchema().getField("header");
        if (headerField != null) {
            IndexedRecord header = (IndexedRecord) msg.get(headerField.pos());
            Schema.Field tsField = headerField.schema().getField("timestamp");
            Schema.Field createdField = headerField.schema().getField("created");

            if (tsField != null) {
                Object ts = header.get(tsField.pos());
                if (ts instanceof Long) {
                    long tsEvent = (Long) ts;
                    metrics.lagFromOriginMs.update(tsNowMs - tsEvent);
                }
            }
            if (createdField != null) {
                Object created = header.get(createdField.pos());
                if (created instanceof Long) {
                    long tsCreated = (Long) created;
                    metrics.lagFromPreviousMs.update(tsNowMs - tsCreated);
                }
            }
        }
    }

    protected GenericData.Record getCamusHeaders(GenericRecord eventHeaders, Schema headerSchema, long tsNow) {
        GenericRecordBuilder builder = new GenericRecordBuilder(headerSchema);
        if (headerSchema.getField("created") != null && !builder.has("created")) {
            builder.set("created", tsNow);
        }
        copyAvroField(eventHeaders, headerSchema, builder, "timestamp");
        copyAvroField(eventHeaders, headerSchema, builder, "id");
        return builder.build();
    }

    private void copyAvroField(GenericRecord src, Schema dstSchema, GenericRecordBuilder dstBuilder, String field) {
        if (dstSchema.getField(field) != null && !dstBuilder.has(field) && src.getSchema().getField(field) != null) {
            dstBuilder.set(field, src.get(field));
        }
    }

    protected int getNumPartitionsForSystemStream(SystemStream systemStream) {
        return KafkaAdmin.getNumPartitionsForStream(config, systemStream);
    }

    protected SystemStream getSystemStream(String logicalStreamName) {
        return new SystemStream(ConfigConst.DEFAULT_SYSTEM_NAME, getStreamName(logicalStreamName));
    }

    protected String getStreamName(String logicalStreamName) {
        String prop = ConfigConst.STREAM_NAME_PREFIX + logicalStreamName;
        String streamName = config.get(prop);
        if (streamName == null) {
            throw new ConfigException("Missing config property for stream: " + prop);
        }
        return streamName;
    }

    //This is for Jython access
    protected Logger getLogger() {
        return this.logger;
    }

    /*
      For testing in the IDE
    */
    public static void run(String jobName) {
        String rootDir = Paths.get(".").toAbsolutePath().normalize().toString();
        String[] params = {
                "--config-factory",
                "org.apache.samza.config.factories.PropertiesConfigFactory",
                "--config-path",
                String.format("file://%s/src/main/config/%s.properties", rootDir, jobName)
        };
        JobRunner.main(params);
    }
}
