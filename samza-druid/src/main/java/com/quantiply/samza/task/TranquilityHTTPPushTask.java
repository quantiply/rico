/*
 * Copyright 2016 Quantiply Corporation. All rights reserved.
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

public class TranquilityHTTPPushTask extends BaseTask {
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
