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
