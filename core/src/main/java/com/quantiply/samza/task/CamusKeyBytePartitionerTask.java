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
import com.quantiply.samza.Partitioner;
import org.apache.samza.config.Config;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;

/*
  This task expects
  - the input topic to have a key present
  - the input topic key to be serialized according the Camus/Confluent Platform spec
  - the input and output topics are configured to use ByteSerde
  - the output topic is specified with the "streams.out" property

  It calculates a partition id using a hash of the key bytes and number of output partitions

 */
public class CamusKeyBytePartitionerTask extends BaseTask {
    private SystemStream outStream;
    private int numOutPartitions;

    @Override
    protected void _init(Config config, TaskContext context, MetricAdaptor metricAdaptor) throws Exception {
        registerDefaultHandler(this::processMsg);
        outStream = getSystemStream("out");
        numOutPartitions = getNumPartitionsForSystemStream(outStream);
    }

    protected void processMsg(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) throws Exception {
        byte[] keyBytes = (byte[])envelope.getKey();
        int partitionId = Partitioner.getPartitionIdForCamus(keyBytes, numOutPartitions);
        collector.send(new OutgoingMessageEnvelope(outStream, partitionId, keyBytes, envelope.getMessage()));
    }
}
