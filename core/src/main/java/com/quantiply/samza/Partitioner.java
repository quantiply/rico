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
package com.quantiply.samza;

import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

public class Partitioner {

    private static Logger logger = LoggerFactory.getLogger(Partitioner.class);

    public static int getPartitionIdForCamus(byte[] camusMsg, int numPartitions) {
        int partitionId = getPartitionId(new CamusFrame(camusMsg).getBody(), numPartitions);
        if (logger.isTraceEnabled()) {
            ByteBuffer bodyBuffer = new CamusFrame(camusMsg).getBody();
            int length = bodyBuffer.remaining();
            byte[] bodyBytes = new byte[length];
            bodyBuffer.get(bodyBytes, 0, length);
            logger.trace(String.format("Camus key msg size %d, body size %s, body bytes %s, partitionId %d",
                    camusMsg.length,
                    bodyBytes.length,
                    javax.xml.bind.DatatypeConverter.printBase64Binary(bodyBytes),
                    partitionId
            ));
        }
        return partitionId;
    }

    public static int getPartitionId(byte[] key, int numPartitions) {
        return getPartitionId(key, 0, key.length, numPartitions);
    }

    public static int getPartitionId(byte[] buffer, int start, int length, int numPartitions) {
        return Utils.abs(murmur2(buffer, start, length)) % numPartitions;
    }

    public static int getPartitionId(ByteBuffer buffer, int numPartitions) {
        if (!buffer.hasArray()) {
            throw new IllegalArgumentException("Buffer is not backed by an array");
        }
        int start = buffer.arrayOffset() + buffer.position();
        int length = buffer.remaining();
        return getPartitionId(buffer.array(), start, length, numPartitions);
    }

    /**
     * Generates 32 bit murmur2 hash from byte array
     * @param data byte array to hash
     * @return 32 bit hash of the given array
     */
    public static int murmur2(final byte[] data) {
        return murmur2(data, 0, data.length);
    }

    /**
     * Generates 32 bit murmur2 hash from byte array
     * @param data ByteBuffer source
     * @param start start offset
     * @param length number of bytes to use
     * @return 32 bit hash of the given array
     *
     * Adapted from Apache Kafka client utils to support ByteBuffer
     */
    public static int murmur2(final byte[] data, int start, int length) {
        int seed = 0x9747b28c;
        // 'm' and 'r' are mixing constants generated offline.
        // They're not really 'magic', they just happen to work well.
        final int m = 0x5bd1e995;
        final int r = 24;

        // Initialize the hash to a random value
        int h = seed ^ length;
        int length4 = length / 4;

        for (int i = 0; i < length4; i++) {
            final int i4 = i * 4;
            int k = (data[start + i4 + 0] & 0xff) + ((data[start + i4 + 1] & 0xff) << 8) + ((data[start + i4 + 2] & 0xff) << 16) + ((data[start + i4 + 3] & 0xff) << 24);
            k *= m;
            k ^= k >>> r;
            k *= m;
            h *= m;
            h ^= k;
        }

        // Handle the last few bytes of the input array
        switch (length % 4) {
            case 3:
                h ^= (data[start + (length & ~3) + 2] & 0xff) << 16;
            case 2:
                h ^= (data[start + (length & ~3) + 1] & 0xff) << 8;
            case 1:
                h ^= (data[start + (length & ~3)] & 0xff);
                h *= m;
        }

        h ^= h >>> 13;
        h *= m;
        h ^= h >>> 15;

        return h;
    }
}
