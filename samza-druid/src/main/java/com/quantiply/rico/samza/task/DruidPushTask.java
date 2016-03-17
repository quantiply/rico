package com.quantiply.rico.samza.task;

import com.quantiply.samza.MetricAdaptor;
import com.quantiply.samza.task.BaseTask;
import org.apache.samza.config.Config;
import org.apache.samza.job.JobRunner;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;

import java.nio.file.Paths;
import java.util.Map;

/***
 * A Noop task for pushing data to druid using tranquility.
 */
public class DruidPushTask extends BaseTask {

    private SystemStream systemStreamOut;

    @Override
    protected void _init(Config config, TaskContext context, MetricAdaptor metricAdaptor) throws Exception {
        systemStreamOut = new SystemStream("druid", "dummy");
        registerDefaultHandler(this::processMsg);
    }

    private void processMsg(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) throws Exception {
        Map<String, Object> outgoingMap = (Map<String, Object>) envelope.getMessage();
        collector.send(new OutgoingMessageEnvelope(systemStreamOut, outgoingMap));
    }

    /* For testing in the IDE.*/
    public static void main(String... args) {
        String rootDir = Paths.get(".").toAbsolutePath().normalize().toString();
        String[] params = {
                "--config-factory",
                "org.apache.samza.config.factories.PropertiesConfigFactory",
                "--config-path",
                String.format("file://%s/samza-druid/src/main/config/%s.properties", rootDir, "tranquility")
        };
        JobRunner.main(params);

    }
}
