package com.quantiply.rico.samza.task;

import com.quantiply.samza.MetricAdaptor;
import com.quantiply.samza.task.BaseTask;
import org.apache.samza.config.Config;
import org.apache.samza.job.JobRunner;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;

import java.nio.file.Paths;

/***
 * A generic task for pushing data to druid via tranquility.
 */
public class DruidPushTask extends BaseTask {

    private SystemStream systemStreamOut;

    @Override
    protected void _init(Config config, TaskContext context, MetricAdaptor metricAdaptor) throws Exception {
        systemStreamOut = new SystemStream("druid", "dummy");
        registerDefaultHandler(this::processMsg);
    }

    private void processMsg(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) throws Exception {
        collector.send(new OutgoingMessageEnvelope(systemStreamOut, envelope.getMessage()));
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
