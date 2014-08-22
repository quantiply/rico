package com.quantiply.rico.common;

import java.io.IOException;
import java.util.Map;

import javax.xml.bind.DatatypeConverter;

import org.junit.*;
import static org.junit.Assert.*;

import com.google.common.collect.ImmutableMap;
import com.quantiply.rico.common.codec.Decoder;
import com.quantiply.rico.common.codec.Encoder;
import com.quantiply.rico.common.codec.RawMessage;
import com.quantiply.schema.Headers;

public class CodecTest {

    protected Encoder getEncoder() {
        return new Encoder();
    }
    
    protected Decoder getDecoder() {
        return new Decoder();
    }
    
    protected Headers getHeaders(Map<String, String> kv) {
        Headers hdrs = Headers.newBuilder()
            .setId("msgId")
            .setOccured("2014-07-23T00:06:00.000Z")
            .build();
        if (kv != null) {
            hdrs.setKv(kv);
        }
        return hdrs;
    }

    @Test(expected = IllegalArgumentException.class)
    public void encodeNullMessage() throws IOException {
        getEncoder().encode(null, getHeaders(null));
    }

    @Test(expected = IllegalArgumentException.class)
    public void encodeNullHeaders() throws IOException {
        getEncoder().encode(new byte[1], null);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testEncodeEmptyMessage() throws IOException {
        byte[] empty = new byte[0];
        getEncoder().encode(empty, getHeaders(null));
    }

    @Test
    public void testEncodeNoHeader() throws IOException {
        String expectedHex = "010A6D736749640030323031342D30372D32335430303A30363A30302E3030305A0020E04FD020EA3A6910A2D808002B30309D";
        String hexMsg = "e04fd020ea3a6910a2d808002b30309d";
        byte[] body = DatatypeConverter.parseHexBinary(hexMsg);

        Encoder encoder = getEncoder();
        byte[] bytes = encoder.encode(body, getHeaders(null));
        String byteStr = DatatypeConverter.printHexBinary(bytes);
       assertEquals(expectedHex, byteStr);
    }

    @Test
    public void testEncodeWithHeaders() throws IOException {
        String expectedHex = "010A6D736749640030323031342D30372D32335430303A30363A30302E3030305A0406666F6F06626172046869066D6F6D0020E04FD020EA3A6910A2D808002B30309D";
        String hexMsg = "e04fd020ea3a6910a2d808002b30309d";
        Map<String, String> kv = ImmutableMap.of("foo", "bar", "hi", "mom");
        byte[] body = DatatypeConverter.parseHexBinary(hexMsg);

        Encoder encoder = getEncoder();
        Headers hdrs = getHeaders(kv);
        final byte[] bytes = encoder.encode(body, hdrs);
        String byteStr = DatatypeConverter.printHexBinary(bytes);
        assertEquals(expectedHex, byteStr);
        
        Decoder decoder = getDecoder();
        RawMessage decoded = decoder.decode(bytes);
        assertArrayEquals(body, decoded.body());
        assertEquals(hdrs, decoded.headers());
        assertEquals(kv, decoded.headers().getKv());
        
        //Making sure Avro object reuse works
        final byte[] bytes2 = encoder.encode(body, getHeaders(kv));
        String byteStr2 = DatatypeConverter.printHexBinary(bytes2);
        assertEquals(expectedHex, byteStr2);

        RawMessage decoded2 = decoder.decode(bytes2);
        assertArrayEquals(body, decoded2.body());
        assertEquals(hdrs, decoded2.headers());
    }

    
}