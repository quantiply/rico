package com.quantiply.samza.task;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.quantiply.rico.errors.ConfigException;
import com.quantiply.samza.MetricAdaptor;
import com.quantiply.samza.util.KafkaAdmin;
import com.quantiply.samza.util.LogContext;
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

import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public abstract class BaseTask implements InitableTask, StreamTask {
    protected LogContext logContext;
    protected Config config;
    protected static Logger logger = LoggerFactory.getLogger(new Object(){}.getClass().getEnclosingClass());

    private final Map<String, StreamMsgHandler> handlerMap = new HashMap<>();
    private Optional<StreamMsgHandler> defaultHandler = Optional.empty();
    private MetricAdaptor metricAdaptor;

    @FunctionalInterface
    public interface StreamMetricFactory<M> {
        public M apply(StreamMetricRegistry registry);
    }

    @FunctionalInterface
    public interface ProcessFunction<M> {
        void apply(IncomingMessageEnvelope t, MessageCollector u, TaskCoordinator s, M m) throws Exception;
    }

    private class StreamMetrics<M> {
        public final Meter processed;
        public final Meter errors;
        public final M custom;

        public StreamMetrics(Optional<String> streamName, MetricAdaptor adaptor, StreamMetricFactory<M> metricFactory) {
            String prefix = streamName.map(s -> s + ".").orElse("");
            processed = adaptor.meter(prefix + "processed");
            errors = adaptor.meter(prefix + "errors");
            custom = metricFactory.apply(new StreamMetricRegistry(prefix, adaptor));
        }
    }

    /*
      M is the custom stream metric class
    */
    private class StreamMsgHandler<M> {
        private Optional<String> name;
        private ProcessFunction<M> processFunc;
        private Optional<SystemStream> errorSystemStream;
        private StreamMetrics<M> metrics;

        public StreamMsgHandler(Optional<String> name, ProcessFunction<M> handler, Optional<SystemStream> errorSystemStream, StreamMetrics<M> metrics) {
            this.name = name;
            this.processFunc = handler;
            this.errorSystemStream = errorSystemStream;
            this.metrics = metrics;
        }

        public Optional<String> getName() {
            return name;
        }

        public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) throws Exception {
            try {
                processFunc.apply(envelope, collector, coordinator, metrics.custom);
                metrics.processed.mark();
            }
            catch (Exception e) {
                metrics.errors.mark();
                if (errorSystemStream.isPresent()) {
                    if (logger.isInfoEnabled()) {
                        logger.info("Error handling message. Sending to error stream.", e);
                    }
                    collector.send(new OutgoingMessageEnvelope(
                            errorSystemStream.get(),
                            envelope.getMessage(),
                            null,
                            envelope.getKey()
                    ));
                }
            }
        }
    }

    @Override
    public void init(Config config, TaskContext context) throws Exception {
        this.config = config;
        logContext = new LogContext(context);
        metricAdaptor = new MetricAdaptor(new MetricRegistry(), context.getMetricsRegistry(), "com.quantiply.rico");
        _init(config, context, metricAdaptor);
    }

    @Override
    public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) throws Exception {
        logContext.setMDC(envelope);

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

        logContext.clearMDC();
    }

    protected abstract void _init(Config config, TaskContext context, MetricAdaptor metricAdaptor) throws Exception;

    protected <M> void registerDefaultHandler(ProcessFunction<M> processFunc, StreamMetricFactory<M> metricFactory) {
        registerDefaultHandler(processFunc, metricFactory, Optional.empty());
    }

    protected <M> void registerDefaultHandler(ProcessFunction<M> processFunc, StreamMetricFactory<M> metricFactory, String logicalErrorStreamName) {
        registerDefaultHandler(processFunc, metricFactory, Optional.of(logicalErrorStreamName));
    }

    protected <M> void registerHandler(String logicalStreamName, ProcessFunction<M> processFunc, StreamMetricFactory<M> metricFactory) {
        registerHandler(logicalStreamName, processFunc, metricFactory, Optional.empty());
    }

    protected <M> void registerHandler(String logicalStreamName, ProcessFunction<M> processFunc, StreamMetricFactory<M> metricFactory, String logicalErrorStreamName) {
        registerHandler(logicalStreamName, processFunc, metricFactory, Optional.of(logicalErrorStreamName));
    }

    protected <M> void registerHandler(String logicalStreamName, ProcessFunction<M> processFunc, StreamMetricFactory<M> metricFactory, Optional<String> logicalErrorStreamName) {
        StreamMsgHandler<M> handler = getStreamMsgHandler(Optional.of(logicalStreamName), processFunc, metricFactory, logicalErrorStreamName);
        handlerMap.put(handler.getName().get(), handler);
    }

    protected <M> void registerDefaultHandler(ProcessFunction<M> processFunc, StreamMetricFactory<M> metricFactory, Optional<String> logicalErrorStreamName) {
        defaultHandler = Optional.of(getStreamMsgHandler(Optional.empty(), processFunc, metricFactory, logicalErrorStreamName));
    }

    private <M> StreamMsgHandler<M> getStreamMsgHandler(Optional<String> logicalStreamName, ProcessFunction<M> processFunc, StreamMetricFactory<M> metricFactory, Optional<String> logicalErrorStreamName) {
        Optional<String> streamName = logicalStreamName.map(s -> getStreamName(s));
        Optional<SystemStream> errorSystemStream = logicalErrorStreamName.map(s -> getSystemStream(s));
        StreamMetrics<M> metrics = new StreamMetrics<>(streamName, metricAdaptor, metricFactory);
        return new StreamMsgHandler<>(streamName, processFunc, errorSystemStream, metrics);
    }

    protected void recordEventLagFromCamusRecord(IndexedRecord msg, long tsNowMs, Histogram histogram) {
        Schema.Field headerField = msg.getSchema().getField("header");
        if (headerField != null) {
            Schema.Field tsField = headerField.schema().getField("timestamp");
            if (tsField != null && tsField.schema().getType() == Schema.Type.LONG) {
                SpecificRecord header = (SpecificRecord) msg.get(headerField.pos());
                long tsEvent = ((Long)header.get(tsField.pos())).longValue();
                histogram.update(tsNowMs - tsEvent);
            }
        }
    }

    protected int getNumPartitionsForSystemStream(SystemStream systemStream) {
        return KafkaAdmin.getNumPartitionsForStream(config, systemStream);
    }

    protected SystemStream getSystemStream(String logicalStreamName) {
        return new SystemStream("kafka", getStreamName(logicalStreamName));
    }

    protected String getStreamName(String logicalStreamName) {
        String prop = "streams." + logicalStreamName;
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
