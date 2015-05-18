package com.quantiply.samza;

import java.nio.ByteBuffer;

public class CamusFrame {
    protected static final byte MAGIC_BYTE = 0x0;
    protected static final int HEAD_SIZE = 5;

    private final byte[] buffer;

    public CamusFrame(byte[] msg) {
        if (msg.length <= 0) {
            throw new IllegalArgumentException("Empty buffer");
        }
        if (msg[0] != MAGIC_BYTE) {
            throw new IllegalArgumentException(String.format("Not a Camus-framed message.  First byte was %X", msg[0]));
        }
        buffer = msg;
    }

    public ByteBuffer getHead() {
        return ByteBuffer.wrap(buffer, 0, HEAD_SIZE).slice();
    }

    public ByteBuffer getBody() {
        return ByteBuffer.wrap(buffer, HEAD_SIZE, buffer.length - HEAD_SIZE).slice();
    }

}
