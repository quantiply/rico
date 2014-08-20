package com.quantiply.rico.common;

import java.io.IOException;
import java.util.Map;

import javax.xml.bind.DatatypeConverter;

import org.junit.Rule;
import org.junit.Test;

import static org.junit.Assert.*;

import org.junit.rules.ExpectedException;

import com.google.common.collect.ImmutableMap;
import com.quantiply.rico.common.codec.Decoder;
import com.quantiply.rico.common.codec.Encoder;
import com.quantiply.rico.common.codec.RawMessage;

public class CodecTest {
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    protected Encoder getEncoder() {
        return new Encoder();
    }
    
    protected Decoder getDecoder() {
        return new Decoder();
    }

    @Test
    public void encodeNullMessage() throws IOException {
        thrown.expect(IllegalArgumentException.class);
        getEncoder().encode(null, null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testEncodeEmptyMessage() throws IOException {
        byte[] empty = new byte[0];
        getEncoder().encode(empty, null);
    }

    @Test
    public void testEncodeNoHeader() throws IOException {
        String expectedHex = "010020E04FD020EA3A6910A2D808002B30309D";
        String hexMsg = "e04fd020ea3a6910a2d808002b30309d";
        byte[] body = DatatypeConverter.parseHexBinary(hexMsg);

        Encoder encoder = getEncoder();
        byte[] bytes = encoder.encode(body, null);
        String byteStr = DatatypeConverter.printHexBinary(bytes);
        assertEquals(expectedHex, byteStr);
    }

    @Test
    public void testEncodeWithHeaders() throws IOException {
        String expectedHex = "010406666F6F06626172046869066D6F6D0020E04FD020EA3A6910A2D808002B30309D";
        String hexMsg = "e04fd020ea3a6910a2d808002b30309d";
        Map<String, String> hdrs = ImmutableMap.of("foo", "bar", "hi", "mom");
        byte[] body = DatatypeConverter.parseHexBinary(hexMsg);

        Encoder encoder = getEncoder();
        final byte[] bytes = encoder.encode(body, hdrs);
        String byteStr = DatatypeConverter.printHexBinary(bytes);
        assertEquals(expectedHex, byteStr);
        
        Decoder decoder = getDecoder();
        RawMessage decoded = decoder.decode(bytes);
        assertArrayEquals(body, decoded.body());
        assertEquals(hdrs, decoded.headers());
        
        //Making sure Avro object reuse works
        final byte[] bytes2 = encoder.encode(body, hdrs);
        String byteStr2 = DatatypeConverter.printHexBinary(bytes2);
        assertEquals(expectedHex, byteStr2);

        RawMessage decoded2 = decoder.decode(bytes2);
        assertArrayEquals(body, decoded2.body());
        assertEquals(hdrs, decoded2.headers());
    }

    
}