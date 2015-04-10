package com.quantiply.samza.task;

import com.quantiply.rico.errors.ConfigException;
import com.quantiply.samza.util.KafkaAdmin;
import com.quantiply.samza.util.LogContext;
import org.apache.samza.config.Config;
import org.apache.samza.job.JobRunner;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.*;

import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public abstract class BaseTask implements InitableTask, StreamTask {
    @FunctionalInterface
    public interface SamzaMsgHandler {
        void apply(IncomingMessageEnvelope t, MessageCollector u, TaskCoordinator s) throws Exception;
    }

    private class HandlerEntry {
        public SamzaMsgHandler handler;
        public Optional<SystemStream> errorSystemStream;

        public HandlerEntry(SamzaMsgHandler handler, Optional<SystemStream> errorSystemStream) {
            this.handler = handler;
            this.errorSystemStream = errorSystemStream;
        }
    }

    protected LogContext logContext;
    protected Config config;
    private final Map<String, HandlerEntry> handlerMap = new HashMap<>();
    private Optional<HandlerEntry> defaultHandler = Optional.empty();

    @Override
    public void init(Config config, TaskContext context) throws Exception {
        this.config = config;
        logContext = new LogContext(context);
        _init(config, context);
    }

    @Override
    public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) throws Exception {
        logContext.setMDC(envelope);

        //Dispatch
        HandlerEntry handlerEntry;
        String streamName = envelope.getSystemStreamPartition().getStream();
        if (handlerMap.containsKey(streamName)) {
            handlerEntry = handlerMap.get(streamName);
        }
        else if (defaultHandler.isPresent()) {
            handlerEntry = defaultHandler.get();
        }
        else {
            throw new ConfigException("No handler for input stream: " + streamName);
        }

        try {
            handlerEntry.handler.apply(envelope, collector, coordinator);
        }
        catch (Exception e) {
            if (handlerEntry.errorSystemStream.isPresent()) {
                collector.send(new OutgoingMessageEnvelope(
                        handlerEntry.errorSystemStream.get(),
                        envelope.getMessage(),
                        null,
                        envelope.getKey()
                ));
            }
        }

        logContext.clearMDC();
    }

    protected abstract void _init(Config config, TaskContext context) throws Exception;

    protected void registerDefaultHandler(SamzaMsgHandler handler) {
        registerDefaultHandler(handler, Optional.empty());
    }

    protected void registerDefaultHandler(SamzaMsgHandler handler, Optional<String> logicalErrorStreamName) {
        Optional<SystemStream> errorSystemStream = logicalErrorStreamName.map(s -> getSystemStream(s));
        defaultHandler = Optional.of(new HandlerEntry(handler, errorSystemStream));
    }

    protected void registerHandler(String logicalStreamName, SamzaMsgHandler handler) throws Exception {
        registerHandler(logicalStreamName, handler, Optional.empty());
    }

    protected void registerHandler(String logicalStreamName, SamzaMsgHandler handler, Optional<String> logicalErrorStreamName) throws Exception {
        String streamName = getStreamName(logicalStreamName);
        Optional<SystemStream> errorSystemStream = logicalErrorStreamName.map(s -> getSystemStream(s));
        handlerMap.put(streamName, new HandlerEntry(handler, errorSystemStream));
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
