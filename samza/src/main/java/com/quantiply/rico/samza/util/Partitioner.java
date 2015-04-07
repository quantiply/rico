package com.quantiply.rico.samza.util;

import org.apache.kafka.common.utils.Utils;

public class Partitioner {

    public static int getPartitionId(byte[] key, int numPartitions) {
        return Utils.abs(Utils.murmur2(key)) % numPartitions;
    }
}
