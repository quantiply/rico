package com.quantiply.samza.util;

import org.apache.kafka.common.utils.Utils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static javax.xml.bind.DatatypeConverter.parseBase64Binary;
import static org.junit.Assert.assertEquals;

public class PartitionerTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    public byte[] getTestBytes() {
        return parseBase64Binary("AAAAAAEUdHAyMjF3MjJtMxxwcmQ6ZXRzOnMyOm9yZBx4UHJkT3B0TWluaUdldA==");
    }

    @Test
    public void testGetPartitionId() throws Exception {
        byte[] bytes = getTestBytes();
        int numPartitions = 12;
        int expectedId = Utils.abs(Utils.murmur2(bytes)) % numPartitions;

        assertEquals(expectedId, Partitioner.getPartitionId(bytes, numPartitions));
        assertEquals(expectedId, Partitioner.getPartitionId(bytes, 0, bytes.length, numPartitions));
    }

    @Test
    public void testMurmur2() throws Exception {
        byte[] bytes = getTestBytes();
        byte[] extra = { 0x0A, 0x01, 0x02, 0x2F, 0x10 };

        byte[] bytesInMiddle = new byte[bytes.length + 2*extra.length];
        int offset = 0;
        System.arraycopy(extra, 0, bytesInMiddle, offset, extra.length);
        offset += extra.length;
        System.arraycopy(bytes, 0, bytesInMiddle, offset, bytes.length);
        offset += bytes.length;
        System.arraycopy(extra, 0, bytesInMiddle, offset, extra.length);

        byte[] bytesAtEnd = new byte[bytes.length + extra.length];
        offset = 0;
        System.arraycopy(extra, 0, bytesAtEnd, offset, extra.length);
        offset += extra.length;
        System.arraycopy(bytes, 0, bytesAtEnd, offset, bytes.length);

        int expected = Utils.murmur2(bytes);

        assertEquals(expected, Partitioner.murmur2(bytes));
        assertEquals(expected, Partitioner.murmur2(bytes, 0, bytes.length));
        assertEquals(expected, Partitioner.murmur2(bytesInMiddle, extra.length, bytes.length));
        assertEquals(expected, Partitioner.murmur2(bytesAtEnd, extra.length, bytes.length));
    }

    @Test
    public void testMurmur2LastByte() {
        byte[] bytes = parseBase64Binary("AAAAAAMUdHAyMjF3MjJtMxxwcmQ6ZXRzOnMyOm9yZBhVc3JHZXRTdGF0dXM=");
        assertEquals(-1934714676, Partitioner.murmur2(bytes, 5, bytes.length - 5));
    }

    @Test
    public void testMurmur2BadStart() throws Exception {
        byte[] bytes = getTestBytes();
        thrown.expect(ArrayIndexOutOfBoundsException.class);
        Partitioner.murmur2(bytes, 1000, 5);
    }
}