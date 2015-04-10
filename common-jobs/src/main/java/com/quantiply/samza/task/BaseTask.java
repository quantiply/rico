package com.quantiply.samza.task;

import com.quantiply.samza.util.LogContext;
import org.apache.samza.config.Config;
import org.apache.samza.job.JobRunner;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.task.*;

import javax.naming.ConfigurationException;
import java.nio.file.Paths;

public class BaseTask implements InitableTask, StreamTask {
    protected LogContext logContext;
    protected Config config;

    @Override
    public void init(Config config, TaskContext context) throws Exception {
        this.config = config;
        logContext = new LogContext(context);
    }

    @Override
    public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) throws Exception {
        logContext.setMDC(envelope);

        //Dispatch
        processDefault(envelope, collector, coordinator);

        logContext.clearMDC();
    }

    public void processDefault(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) throws Exception {
        String streamName = envelope.getSystemStreamPartition().getStream();
        throw new ConfigurationException(
                String.format("No handler found for stream; %s.  Override the processDefault method or register a handler",
                        streamName));
    }

    protected String getStreamName(String logicalName) throws Exception {
        String prop = "streams." + logicalName;
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
