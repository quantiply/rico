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

import com.quantiply.samza.partition.CamusFrame;
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