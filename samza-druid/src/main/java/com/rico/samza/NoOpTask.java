package com.rico.samza;

import org.apache.samza.job.JobRunner;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskCoordinator;

import java.nio.file.Paths;
import java.util.Map;

/**
 * Created by sarrawat on 3/15/16.
 */
public class NoOpTask implements StreamTask {
    private static final SystemStream OUTPUT_STREAM = new SystemStream("druid", "clicks");

    @Override
    public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) {
        Map<String, Object> outgoingMap = (Map<String, Object>) envelope.getMessage();
        collector.send(new OutgoingMessageEnvelope(OUTPUT_STREAM, outgoingMap));
    }

    public static void main(String... args) {
        String rootDir = Paths.get(".").toAbsolutePath().normalize().toString();
        String[] params = {
                "--config-factory",
                "org.apache.samza.config.factories.PropertiesConfigFactory",
                "--config-path",
                String.format("file://%s/src/main/config/%s.properties", rootDir, "tranquility")
        };
        JobRunner.main(params);

    }
}
