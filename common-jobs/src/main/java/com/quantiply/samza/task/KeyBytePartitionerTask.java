package com.quantiply.samza.task;

import com.quantiply.samza.util.KafkaAdmin;
import com.quantiply.samza.util.LogContext;
import com.quantiply.samza.util.Partitioner;
import org.apache.samza.config.Config;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
  This task expects
  - the input topic to have a key present
  - the input and output topics are configured to use ByteSerde
  - the output topic is specified with the "topics.out" property

  It calculates a partition id using a hash of the key bytes and number of output partitions

 */
public class KeyBytePartitionerTask implements StreamTask, InitableTask {
    private static Logger logger = LoggerFactory.getLogger(KeyBytePartitionerTask.class);
    private SystemStream outStream;
    private int numOutPartitions;
    private LogContext logContext;

    @Override
    public void init(Config config, TaskContext context) throws Exception {
        logContext = new LogContext(context);
        String outTopic = config.get("topics.out");
        if (outTopic == null) {
            throw new IllegalArgumentException("Missing property for output topic: topics.out");
        }
        outStream = new SystemStream("kafka", outTopic);
        numOutPartitions = KafkaAdmin.getNumPartitionsForStream(config, outTopic);
    }

    @Override
    public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) throws Exception {
        logContext.setMDC(envelope);

        byte[] keyBytes = (byte[])envelope.getKey();
        int partition = Partitioner.getPartitionId(keyBytes, numOutPartitions);

        if (logger.isTraceEnabled()) {
                logger.trace(String.format("Bytes %s, num partitions %d, id %d",
                        javax.xml.bind.DatatypeConverter.printBase64Binary(keyBytes),
                        numOutPartitions,
                        Partitioner.getPartitionId(keyBytes, numOutPartitions)
                ));
        }

        collector.send(new OutgoingMessageEnvelope(outStream, partition, keyBytes, envelope.getMessage()));

        logContext.clearMDC();
    }
}
