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
import com.quantiply.rico.samza.DroppedMessage;
import com.quantiply.samza.MetricAdaptor;
import com.quantiply.samza.metrics.StreamMetricFactory;
import com.quantiply.samza.serde.AvroSerde;
import com.quantiply.samza.serde.AvroSerdeFactory;
import com.quantiply.samza.metrics.EventStreamMetrics;
import com.quantiply.samza.admin.KafkaAdmin;
import com.quantiply.samza.admin.TaskInfo;
import com.quantiply.samza.metrics.StreamMetricRegistry;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.specific.SpecificRecord;
import org.apache.samza.config.Config;
import org.apache.samza.config.ConfigException;
import org.apache.samza.job.JobRunner;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.task.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public abstract class BaseTask implements InitableTask, StreamTask {
    protected final static String METRICS_GROUP_NAME = "com.quantiply.rico";
    protected final static String CFG_STREAM_NAME_PREFIX = "rico.streams.";
    protected final static String CFG_DROP_ON_ERROR = "rico.drop.on.error";
    protected final static String CFG_DROPPED_MESSAGE_STREAM_NAME = "rico.streams.dropped-messages";
    protected final static String CFG_DEFAULT_SYSTEM_NAME = "kafka";

    protected TaskInfo taskInfo;
    protected Config config;
    protected static Logger logger = LoggerFactory.getLogger(new Object(){}.getClass().getEnclosingClass());

    private final Map<String, StreamMsgHandler> handlerMap = new HashMap<>();
    private Optional<StreamMsgHandler> defaultHandler = Optional.empty();
    private MetricAdaptor metricAdaptor;
    private boolean dropOnError;
    private Optional<SystemStream> droppedMsgStream;
    private AvroSerde avroSerde;

    @FunctionalInterface
    public interface Process {
        void apply(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) throws Exception;
    }

    @FunctionalInterface
    public interface ProcessWithMetrics<M> {
        void apply(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator, M customMetrics) throws Exception;
    }

    private class StreamMetrics {
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
                if (!dropOnError) {
                    throw e;
                }
                if (logger.isInfoEnabled()) {
                    //NOTE - logging at info level because these can be too numerous in PRD
                    logger.info("Error handling message", e);
                }
                metrics.dropped.mark();

                if (droppedMsgStream.isPresent()) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Sending message to dropped message stream: " + droppedMsgStream.get().getStream());
                    }
                    DroppedMessage droppedMessage = DroppedMessage.newBuilder()
                            .setError(Optional.ofNullable(e.getMessage()).orElse(""))
                            .setKey(Optional.ofNullable((byte[]) envelope.getKey()).map(ByteBuffer::wrap).orElse(null))
                            .setMessage(ByteBuffer.wrap((byte[]) envelope.getMessage()))
                            .setSystem(envelope.getSystemStreamPartition().getSystem())
                            .setStream(envelope.getSystemStreamPartition().getStream())
                            .setPartition(envelope.getSystemStreamPartition().getPartition().getPartitionId())
                            .setOffset(envelope.getOffset())
                            .setTask(taskInfo.getTaskName())
                            .setContainer(taskInfo.getContainerName())
                            .setJob(taskInfo.getJobName())
                            .setJobId(taskInfo.getJobId())
                            .build();
                    collector.send(new OutgoingMessageEnvelope(droppedMsgStream.get(), avroSerde.toBytes(droppedMessage)));
                }
            }
        }
    }

    @Override
    public void init(Config config, TaskContext context) throws Exception {
        this.config = config;
        taskInfo = new TaskInfo(config, context);
        metricAdaptor = new MetricAdaptor(new MetricRegistry(), context.getMetricsRegistry(), METRICS_GROUP_NAME);
        dropOnError = config.getBoolean(CFG_DROP_ON_ERROR, false);
        droppedMsgStream = Optional.ofNullable(config.get(CFG_DROPPED_MESSAGE_STREAM_NAME))
                .map(streamName -> new SystemStream(CFG_DEFAULT_SYSTEM_NAME, streamName));
        avroSerde = new AvroSerdeFactory().getSerde(null, config);

        _init(config, context, metricAdaptor);

        validateHandlers(context);
        logDroppedMsgConfig();
    }

    private void validateHandlers(TaskContext context) {
        if (defaultHandler.isPresent()) {
            return;
        }
        for (SystemStreamPartition ssp: context.getSystemStreamPartitions()) {
            if (!handlerMap.containsKey(ssp.getStream())) {
                throw new ConfigException("No handler for stream: " + ssp.getStream());
            }
        }
    }

    private void logDroppedMsgConfig() {
        StringBuilder builder = new StringBuilder();
        if (dropOnError) {
            builder.append("Dropping messages on error.");
            if (droppedMsgStream.isPresent()) {
                builder.append(" Sending to stream: " + droppedMsgStream.get().getStream());
            }
            else {
                builder.append(" No drop stream configured with property: " + CFG_DROPPED_MESSAGE_STREAM_NAME);
            }
        }
        else {
            builder.append("Not dropping messages on error");
        }
        logger.info(builder.toString());
    }

    @Override
    public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) throws Exception {
        taskInfo.setMDC(envelope);

        //Dispatch
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

        handler.process(envelope, collector, coordinator);

        taskInfo.clearMDC();
    }

    protected abstract void _init(Config config, TaskContext context, MetricAdaptor metricAdaptor) throws Exception;

    protected void registerDefaultHandler(Process processFunc) {
        defaultHandler = Optional.of(getStreamMsgHandler(Optional.empty(), processFunc));
    }

    protected <M> void registerDefaultHandler(ProcessWithMetrics<M> processFunc, StreamMetricFactory<M> metricFactory) {
        defaultHandler = Optional.of(getStreamMsgHandler(Optional.empty(), processFunc, metricFactory));
    }

    protected void registerHandler(String logicalStreamName, Process processFunc) {
        String streamName = getStreamName(logicalStreamName);
        StreamMsgHandler handler = getStreamMsgHandler(Optional.of(streamName), processFunc);
        handlerMap.put(handler.getName().get(), handler);
    }

    protected <M> void registerHandler(String logicalStreamName, ProcessWithMetrics<M> processFunc, StreamMetricFactory<M> metricFactory) {
        StreamMsgHandler handler = getStreamMsgHandler(Optional.of(logicalStreamName), processFunc, metricFactory);
        handlerMap.put(handler.getName().get(), handler);
    }

    private <M> StreamMsgHandler getStreamMsgHandler(Optional<String> logicalStreamName, ProcessWithMetrics<M> processWithMetrics, StreamMetricFactory<M> metricFactory) {
        Optional<String> streamName = logicalStreamName.map(this::getStreamName);
        M custom = metricFactory.create(new StreamMetricRegistry(getStreamMetricPrefix(streamName), metricAdaptor));
        Process process = (envelope, collector, coordinator) -> processWithMetrics.apply(envelope, collector, coordinator, custom);
        return getStreamMsgHandler(streamName, process);
    }

    private StreamMsgHandler getStreamMsgHandler(Optional<String> streamName, Process process) {
        StreamMetrics metrics = new StreamMetrics(new StreamMetricRegistry(getStreamMetricPrefix(streamName), metricAdaptor));
        return new StreamMsgHandler(streamName, process, metrics);
    }

    private String getStreamMetricPrefix(Optional<String> streamName) {
        String metricName = streamName.map(StreamMetricRegistry::sanitizeStreamNameForMetrics).orElse("default");
        return String.format("streams.%s.", metricName);
    }

    protected void updateLagMetricsForCamusRecord(IndexedRecord msg, long tsNowMs, EventStreamMetrics metrics) {
        Schema.Field headerField = msg.getSchema().getField("header");
        if (headerField != null) {
            SpecificRecord header = (SpecificRecord) msg.get(headerField.pos());
            Schema.Field tsField = headerField.schema().getField("timestamp");
            Schema.Field createdField = headerField.schema().getField("created");

            if (tsField != null && tsField.schema().getType() == Schema.Type.LONG) {
                long tsEvent = (Long) header.get(tsField.pos());
                metrics.lagFromOriginMs.update(tsNowMs - tsEvent);
            }
            if (createdField != null && createdField.schema().getType() == Schema.Type.LONG) {
                long tsCreated = (Long) header.get(createdField.pos());
                metrics.lagFromPreviousMs.update(tsNowMs - tsCreated);
            }
        }
    }

    protected int getNumPartitionsForSystemStream(SystemStream systemStream) {
        return KafkaAdmin.getNumPartitionsForStream(config, systemStream);
    }

    protected SystemStream getSystemStream(String logicalStreamName) {
        return new SystemStream(CFG_DEFAULT_SYSTEM_NAME, getStreamName(logicalStreamName));
    }

    protected String getStreamName(String logicalStreamName) {
        String prop = CFG_STREAM_NAME_PREFIX + logicalStreamName;
        String streamName = config.get(prop);
        if (streamName == null) {
            throw new ConfigException("Missing config property for stream: " + prop);
        }
        return streamName;
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
