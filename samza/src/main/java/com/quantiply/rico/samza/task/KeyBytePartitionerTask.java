package com.quantiply.rico.samza.task;

import com.quantiply.rico.errors.ConfigException;
import com.quantiply.rico.samza.util.Partitioner;
import org.apache.samza.config.Config;
import org.apache.samza.system.*;
import org.apache.samza.system.kafka.KafkaSystemFactory;
import org.apache.samza.task.*;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

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
        numOutPartitions = getNumPartitionsForOutStream(config, outTopic);
    }

    @Override
    public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) throws Exception {
        byte[] key = (byte[])envelope.getKey();
        int partition = Partitioner.getPartitionId(key, numOutPartitions);
        collector.send(new OutgoingMessageEnvelope(outStream, partition, key, envelope.getMessage()));
    }

    private int getNumPartitionsForOutStream(Config cfg, String outTopic) {
        Set<String> streamNames = new HashSet<String>();
        streamNames.add(outTopic);
        SystemAdmin kafkaSystemAdmin = new KafkaSystemFactory().getAdmin("kafka",
                cfg);
        Map<String, SystemStreamMetadata> metadata =
                kafkaSystemAdmin.getSystemStreamMetadata(streamNames);
        SystemStreamMetadata topicMetadata = metadata.get(outTopic);
        return topicMetadata.getSystemStreamPartitionMetadata().size();
    }
}
