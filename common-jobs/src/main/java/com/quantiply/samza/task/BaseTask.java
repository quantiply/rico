package com.quantiply.samza.task;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.quantiply.rico.errors.ConfigException;
import com.quantiply.rico.samza.DroppedMessage;
import com.quantiply.samza.MetricAdaptor;
import com.quantiply.samza.serde.AvroSerde;
import com.quantiply.samza.serde.AvroSerdeFactory;
import com.quantiply.samza.util.KafkaAdmin;
import com.quantiply.samza.util.TaskInfo;
import com.quantiply.samza.util.StreamMetricRegistry;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.specific.SpecificRecord;
import org.apache.samza.config.Config;
import org.apache.samza.job.JobRunner;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public abstract class BaseTask implements InitableTask, StreamTask {
    protected final static String METRICS_GROUP_NAME = "com.quantiply.rico";
    protected final static String CFG_STREAM_NAME_PREFIX = "streams.";
    protected final static String CFG_DROP_ON_ERROR = "rico.drop.on.error";
    protected final static String CFG_DROPPED_MESSAGE_STREAM_NAME = "streams.dropped-messages";
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
    public interface StreamMetricFactory<M> {
        public M apply(StreamMetricRegistry registry);
    }

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

        public StreamMetrics(String prefix, MetricAdaptor adaptor) {
            processed = adaptor.meter(prefix + "processed");
            dropped = adaptor.meter(prefix + "dropped");
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
                    /*
                    HACK WARNING - getting job + container metadata from MDC is a bit of a hack
                    TODO - Is there a better way to get that info or to log dropped messages?
                     */
                    DroppedMessage droppedMessage = DroppedMessage.newBuilder()
                            .setError(e.getMessage())
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

        logDroppedMsgConfig();
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
        M custom = metricFactory.apply(new StreamMetricRegistry(getStreamMetricPrefix(streamName), metricAdaptor));
        Process process = (envelope, collector, coordinator) -> processWithMetrics.apply(envelope, collector, coordinator, custom);
        return getStreamMsgHandler(streamName, process);
    }

    private StreamMsgHandler getStreamMsgHandler(Optional<String> streamName, Process process) {
        StreamMetrics metrics = new StreamMetrics(getStreamMetricPrefix(streamName), metricAdaptor);
        return new StreamMsgHandler(streamName, process, metrics);
    }

    private String getStreamMetricPrefix(Optional<String> streamName) {
        return streamName.map(s -> s + ".").orElse("");
    }

    protected void recordEventLagFromCamusRecord(IndexedRecord msg, long tsNowMs, Histogram histogram) {
        Schema.Field headerField = msg.getSchema().getField("header");
        if (headerField != null) {
            Schema.Field tsField = headerField.schema().getField("timestamp");
            if (tsField != null && tsField.schema().getType() == Schema.Type.LONG) {
                SpecificRecord header = (SpecificRecord) msg.get(headerField.pos());
                long tsEvent = (Long) header.get(tsField.pos());
                histogram.update(tsNowMs - tsEvent);
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
