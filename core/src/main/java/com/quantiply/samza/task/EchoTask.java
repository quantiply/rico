/*
 * Copyright 2014-2015 Quantiply Corporation. All rights reserved.
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
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;

/**
 * Basic Samza task used for integration testing
 */
public class EchoTask extends BaseTask {
    private SystemStream systemStreamOut;

    @Override
    protected void _init(Config config, TaskContext context, MetricAdaptor metricAdaptor) throws Exception {
        systemStreamOut = getSystemStream("out");
        registerDefaultHandler(this::processMsg);
    }

    private void processMsg(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) throws Exception {
        collector.send(new OutgoingMessageEnvelope(systemStreamOut, null, envelope.getKey(), envelope.getMessage()));
    }
}
