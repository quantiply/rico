package com.quantiply.samza.task;

import com.quantiply.samza.util.Partitioner;
import org.apache.samza.config.Config;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
  This task expects
  - the input topic to have a key present
  - the input topic key to be serialized according the Camus/Confluent Platform spec
  - the input and output topics are configured to use ByteSerde
  - the output topic is specified with the "streams.out" property

  It calculates a partition id using a hash of the key bytes and number of output partitions

 */
public class CamusKeyBytePartitionerTask extends BaseTask {
    private static Logger logger = LoggerFactory.getLogger(CamusKeyBytePartitionerTask.class);
    private SystemStream outStream;
    private int numOutPartitions;

    @Override
    protected void _init(Config config, TaskContext context) throws Exception {
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
