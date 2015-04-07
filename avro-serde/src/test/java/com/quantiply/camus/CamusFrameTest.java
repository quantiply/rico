package com.quantiply.camus;

import org.junit.Test;

import java.nio.ByteBuffer;

import static javax.xml.bind.DatatypeConverter.parseBase64Binary;
import static org.junit.Assert.*;

public class CamusFrameTest {

    public byte[] getCamusBytes() {
        return parseBase64Binary("AAAAAAEUdHAyMjF3MjJtMxxwcmQ6ZXRzOnMyOm9yZBx4UHJkT3B0TWluaUdldA==");
    }

    @Test
    public void testWrappers() throws Exception {
        byte[] msg = getCamusBytes();
        assertEquals(46, msg.length);
        CamusFrame frame = new CamusFrame(msg);
        ByteBuffer head = frame.getHead();
        ByteBuffer body = frame.getBody();
        assertEquals(5, head.remaining());
        assertEquals(0, head.position());
        assertEquals(0, head.arrayOffset());
        assertEquals(5, head.limit());
        assertEquals(41, body.remaining());
        assertEquals(0, body.position());
        assertEquals(5, body.arrayOffset());
        assertEquals(41, body.limit());
    }
}