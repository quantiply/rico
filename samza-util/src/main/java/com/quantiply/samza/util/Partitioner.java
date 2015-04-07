package com.quantiply.samza.util;

import org.apache.kafka.common.utils.Utils;

import java.nio.ByteBuffer;

public class Partitioner {

    public static int getPartitionId(byte[] key, int numPartitions) {
        return Utils.abs(murmur2(key)) % numPartitions;
    }

    public static int getPartitionId(ByteBuffer buffer, int start, int length, int numPartitions) {
        return Utils.abs(murmur2(buffer, start, length)) % numPartitions;
    }

    /**
     * Generates 32 bit murmur2 hash from byte array
     * @param data byte array to hash
     * @return 32 bit hash of the given array
     */
    public static int murmur2(final byte[] data) {
        return murmur2Impl(data, 0, data.length);
    }

    /**
     * Generates 32 bit murmur2 hash from byte array
     * @param buffer ByteBuffer source
     * @param start start offset
     * @param length number of bytes to use
     * @return 32 bit hash of the given array
     */
    public static int murmur2(final ByteBuffer buffer, int start, int length) {
        if (!buffer.hasArray()) {
            throw new IllegalArgumentException("Buffer is not backed by an array");
        }
        if (start < 0 || start >= buffer.array().length) {
            throw new IllegalArgumentException("Invalid start offset: " + start);
        }
        if (length > buffer.array().length - start) {
            throw new IllegalArgumentException("Invalid length: " + length);
        }
        return murmur2Impl(buffer.array(), start, length);
    }

    /*
     *  Adapted from Apache Kafka client utils to support ByteBuffer
     */
    private static int murmur2Impl(final byte[] data, int start, int length) {
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
                h ^= (data[start + length & ~3] & 0xff);
                h *= m;
        }

        h ^= h >>> 13;
        h *= m;
        h ^= h >>> 15;

        return h;
    }
}
