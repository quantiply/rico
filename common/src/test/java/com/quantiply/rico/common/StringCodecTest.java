package com.quantiply.rico.common;

import java.io.IOException;

import org.joda.time.DateTime;
import org.joda.time.format.ISODateTimeFormat;
import org.junit.*;

import static org.junit.Assert.*;

import com.quantiply.rico.common.codec.Headers;
import com.quantiply.rico.common.codec.StringDecoder;
import com.quantiply.rico.common.codec.StringEncoder;
import com.quantiply.rico.common.codec.StringMessage;

public class StringCodecTest {

    protected Headers getHeaders() {
        DateTime occurred = ISODateTimeFormat.dateTime().parseDateTime("2014-07-23T00:06:00.000Z");
        return new Headers("msgId", occurred);
    }
    
    @Test
    public void codec() throws IOException {
        final String content = "hello";
        Headers hdrs = getHeaders();
        StringMessage msg = new StringMessage(content, hdrs);
        
        StringEncoder encoder = new StringEncoder();
        final byte[] bytes = encoder.encode(msg);
        
        StringDecoder decoder = new StringDecoder();
        StringMessage decoded = decoder.decode(bytes);
        
        assertEquals(hdrs, decoded.getHeaders());
        assertEquals(content, decoded.getBody());
    }
}
