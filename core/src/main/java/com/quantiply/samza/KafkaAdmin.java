package com.quantiply.samza;

import org.apache.samza.config.Config;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.SystemStreamMetadata;
import org.apache.samza.system.kafka.KafkaSystemFactory;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class KafkaAdmin {

    public static int getNumPartitionsForStream(Config cfg, SystemStream systemStream) {
        Set<String> streamNames = new HashSet<>();
        streamNames.add(systemStream.getStream());
        SystemAdmin kafkaSystemAdmin = new KafkaSystemFactory().getAdmin(systemStream.getSystem(), cfg);
        Map<String, SystemStreamMetadata> metadata =
                kafkaSystemAdmin.getSystemStreamMetadata(streamNames);
        SystemStreamMetadata topicMetadata = metadata.get(systemStream.getStream());
        return topicMetadata.getSystemStreamPartitionMetadata().size();
    }
}
