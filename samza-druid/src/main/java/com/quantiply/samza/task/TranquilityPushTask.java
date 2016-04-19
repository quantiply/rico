package com.quantiply.samza.task;

import com.quantiply.samza.MetricAdaptor;
import org.apache.samza.config.Config;
import org.apache.samza.job.JobRunner;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;

import java.nio.file.Paths;

public class TranquilityPushTask extends BaseTask {
  protected SystemStream druidStream;

  @Override
  protected void _init(Config config, TaskContext context, MetricAdaptor metricAdaptor) throws Exception {
    if (getErrorHandler().dropOnError()) {
      logger.warn("Task is configured to drop messages on error");
    }
    druidStream = new SystemStream("tranquility", "http");
    registerDefaultHandler(this::handleMsg);
  }

  private void handleMsg(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) {
    collector.send(new OutgoingMessageEnvelope(druidStream, envelope.getKey(), envelope.getMessage()));
  }

  /*
  *    For testing in the IDE
  */
  public static void main(String [] args) {
    String jobName = "tranquility";
    String rootDir = Paths.get(".").toAbsolutePath().normalize().toString();
    String[] params = {
        "--config-factory",
        "org.apache.samza.config.factories.PropertiesConfigFactory",
        "--config-path",
        String.format("file://%s/samza-druid/src/main/config/%s.properties", rootDir, jobName)
    };
    JobRunner.main(params);
  }
}
