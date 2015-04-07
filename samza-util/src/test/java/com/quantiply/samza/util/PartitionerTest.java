package com.quantiply.samza.util;

import org.apache.kafka.common.utils.Utils;
import org.junit.Test;

import java.nio.ByteBuffer;

import static javax.xml.bind.DatatypeConverter.parseBase64Binary;
import static org.junit.Assert.*;

public class PartitionerTest {

    public byte[] getTestBytes() {
        return parseBase64Binary("AAAAAAEUdHAyMjF3MjJtMxxwcmQ6ZXRzOnMyOm9yZBx4UHJkT3B0TWluaUdldA==");
    }

    @Test
    public void testGetPartitionId() throws Exception {
        byte[] bytes = getTestBytes();
        int numPartitions = 12;
        int expectedId = Utils.abs(Utils.murmur2(bytes)) % numPartitions;

        assertEquals(expectedId, Partitioner.getPartitionId(bytes, numPartitions));
        assertEquals(expectedId, Partitioner.getPartitionId(ByteBuffer.wrap(bytes), 0, bytes.length, numPartitions));
    }

    @Test
    public void testMurmur2() throws Exception {
        byte[] bytes = getTestBytes();
        byte[] extra = { 0x0A, 0x01, 0x02, 0x2F };

        ByteBuffer testBytes = ByteBuffer.wrap(bytes);

        byte[] bytesInMiddle = new byte[bytes.length + 2*extra.length];
        int offset = 0;
        System.arraycopy(extra, 0, bytesInMiddle, offset, extra.length);
        offset += extra.length;
        System.arraycopy(bytes, 0, bytesInMiddle, offset, bytes.length);
        offset += bytes.length;
        System.arraycopy(extra, 0, bytesInMiddle, offset, extra.length);

        ByteBuffer testBytesInMiddle = ByteBuffer.wrap(bytesInMiddle);

        int expected = Utils.murmur2(bytes);

        assertEquals(expected, Partitioner.murmur2(bytes));
        assertEquals(expected, Partitioner.murmur2(testBytes, 0, bytes.length));
        assertEquals(expected, Partitioner.murmur2(testBytesInMiddle, extra.length, bytes.length));
    }
}