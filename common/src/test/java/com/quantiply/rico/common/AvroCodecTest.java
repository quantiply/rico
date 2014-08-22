package com.quantiply.rico.common;

import java.io.IOException;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.junit.*;

import static org.junit.Assert.*;

import com.quantiply.rico.common.codec.AvroDecoder;
import com.quantiply.rico.common.codec.AvroEncoder;
import com.quantiply.rico.common.codec.AvroMessage;
import com.quantiply.schema.Headers;
import com.quantiply.schema.test.Fubar;

public class AvroCodecTest {

    protected AvroEncoder getEncoder() {
        return new AvroEncoder();
    }
    
    protected Headers getHeaders() {
        return Headers.newBuilder()
            .setId("msgId")
            .setOccured("2014-07-23T00:06:00.000Z")
            .setSchemaId("fakeSchemaId")
            .build();
    }
    
    @Test
    public void codecSpecificRecord() throws IOException {
        Fubar origRec = Fubar.newBuilder().setBar(7).setFoo("hi").build();
        
        Headers hdrs = getHeaders();
        AvroMessage<Fubar> msg = new AvroMessage<Fubar>(origRec, hdrs);
        
        AvroEncoder encoder = getEncoder();
        final byte[] bytes = encoder.encode(msg);
        
        AvroDecoder<Fubar> decoder = new AvroDecoder<Fubar>(Fubar.class);
        AvroMessage<Fubar> decoded = decoder.decode(bytes, origRec.getSchema());
        
        assertEquals(hdrs, decoded.headers());
        assertEquals(origRec, decoded.body());
    }
    
    @Test
    public void codecGenericRecord() throws IOException {
        GenericRecord origRec = new GenericRecordBuilder(Fubar.getClassSchema())
            .set("bar", new Integer(6))
            .set("foo", "booyah")
            .build();
        
        AvroMessage<GenericRecord> msg = new AvroMessage<GenericRecord>(origRec, getHeaders());
        
        AvroEncoder encoder = getEncoder();
        final byte[] bytes = encoder.encode(msg);
        
        AvroDecoder<GenericRecord> decoder = new AvroDecoder<GenericRecord>(GenericRecord.class);
        AvroMessage<GenericRecord> decoded = decoder.decode(bytes, origRec.getSchema());
        
        assertEquals(origRec, decoded.body());
    }
}
