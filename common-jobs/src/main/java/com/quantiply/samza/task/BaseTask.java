package com.quantiply.samza.task;

import com.quantiply.samza.util.KafkaAdmin;
import com.quantiply.samza.util.LogContext;
import org.apache.samza.config.Config;
import org.apache.samza.job.JobRunner;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.*;

import javax.naming.ConfigurationException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

public class BaseTask implements InitableTask, StreamTask {
    @FunctionalInterface
    public interface SamzaMsgHandler {
        void apply(IncomingMessageEnvelope t, MessageCollector u, TaskCoordinator s) throws Exception;
    }

    protected LogContext logContext;
    protected Config config;
    private final Map<String, SamzaMsgHandler> handlerMap = new HashMap<>();

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
        String streamName = envelope.getSystemStreamPartition().getStream();
        if (handlerMap.containsKey(streamName)) {
            handlerMap.get(streamName).apply(envelope, collector, coordinator);
        }
        else {
            processDefault(envelope, collector, coordinator);
        }

        logContext.clearMDC();
    }

    protected void _init(Config config, TaskContext context) throws Exception {}

    protected void registerHandler(String logicalName, SamzaMsgHandler handler) throws Exception {
        String streamName = getStreamName(logicalName);
        handlerMap.put(streamName, handler);
    }

    protected void processDefault(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) throws Exception {
        String streamName = envelope.getSystemStreamPartition().getStream();
        throw new ConfigurationException(
                String.format("No handler found for stream; %s.  Override the processDefault method or register a handler",
                        streamName));
    }

    protected int getNumPartitionsForSystemStream(SystemStream systemStream) {
        return KafkaAdmin.getNumPartitionsForStream(config, systemStream);
    }

    protected SystemStream getSystemStream(String logicalStreamName) throws ConfigurationException {
        return new SystemStream("kafka", getStreamName(logicalStreamName));
    }

    protected String getStreamName(String logicalStreamName) throws ConfigurationException {
        String prop = "streams." + logicalStreamName;
        String streamName = config.get(prop);
        if (streamName == null) {
            throw new ConfigurationException("Missing config property for stream: " + prop);
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
