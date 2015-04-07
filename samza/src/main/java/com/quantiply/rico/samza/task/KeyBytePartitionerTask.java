package com.quantiply.rico.samza.task;

import com.quantiply.rico.errors.ConfigException;
import com.quantiply.rico.samza.util.KafkaAdmin;
import com.quantiply.rico.samza.util.Partitioner;
import org.apache.samza.config.Config;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.*;

/*
  This task expects
  - the input topic to have a key present
  - the input and output topics are configured to use ByteSerde
  - the output topic is specified with the "topics.out" property

  It calculates a partition id using a hash of the key bytes and number of output partitions

 */
public class KeyBytePartitionerTask implements StreamTask, InitableTask {
    private SystemStream outStream;
    private int numOutPartitions;

    @Override
    public void init(Config config, TaskContext context) throws Exception {
        String outTopic = config.get("topics.out");
        if (outTopic == null) {
            throw new ConfigException("Missing property for output topic: topics.out");
        }
        outStream = new SystemStream("kafka", outTopic);
        numOutPartitions = KafkaAdmin.getNumPartitionsForStream(config, outTopic);
    }

    @Override
    public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) throws Exception {
        byte[] key = (byte[])envelope.getKey();
        int partition = Partitioner.getPartitionId(key, numOutPartitions);
        collector.send(new OutgoingMessageEnvelope(outStream, partition, key, envelope.getMessage()));
    }
}
