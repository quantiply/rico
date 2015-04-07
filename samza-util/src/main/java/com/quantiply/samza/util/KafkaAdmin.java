package com.quantiply.samza.util;

import org.apache.samza.config.Config;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemStreamMetadata;
import org.apache.samza.system.kafka.KafkaSystemFactory;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class KafkaAdmin {

    public static int getNumPartitionsForStream(Config cfg, String stream) {
        Set<String> streamNames = new HashSet<String>();
        streamNames.add(stream);
        SystemAdmin kafkaSystemAdmin = new KafkaSystemFactory().getAdmin("kafka", cfg);
        Map<String, SystemStreamMetadata> metadata =
                kafkaSystemAdmin.getSystemStreamMetadata(streamNames);
        SystemStreamMetadata topicMetadata = metadata.get(stream);
        return topicMetadata.getSystemStreamPartitionMetadata().size();
    }
}
