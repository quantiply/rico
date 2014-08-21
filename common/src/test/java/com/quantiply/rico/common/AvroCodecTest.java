package com.quantiply.rico.common;

import java.io.IOException;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.junit.Rule;
import org.junit.Test;

import static org.junit.Assert.*;

import org.junit.rules.ExpectedException;

import com.google.common.collect.ImmutableMap;
import com.quantiply.rico.common.codec.AvroDecoder;
import com.quantiply.rico.common.codec.AvroEncoder;
import com.quantiply.rico.common.codec.AvroMessage;
import com.quantiply.schema.test.Fubar;

public class AvroCodecTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    protected AvroEncoder getEncoder() {
        return new AvroEncoder();
    }
    
    @Test
    public void codecSpecificRecord() throws IOException {
        String schemaId = "fakeId";
        Fubar origRec = Fubar.newBuilder().setBar(7).setFoo("hi").build();
        
        AvroMessage<Fubar> msg = new AvroMessage<Fubar>(schemaId, origRec);
        
        AvroEncoder encoder = getEncoder();
        final byte[] bytes = encoder.encode(msg);
        
        AvroDecoder<Fubar> decoder = new AvroDecoder<Fubar>(Fubar.class);
        AvroMessage<Fubar> decoded = decoder.decode(bytes, origRec.getSchema(), schemaId);
        
        Map<String, String> expectedHdrs = ImmutableMap.of("Content-Type", "application/x-avro-binary", "X-Rico-Schema-Id", schemaId);
        assertEquals(expectedHdrs, decoded.headers());
        assertEquals(origRec, decoded.body());
    }
    
    @Test
    public void codecGenericRecord() throws IOException {
        String schemaId = "fakeId";
        GenericRecord origRec = new GenericRecordBuilder(Fubar.getClassSchema())
            .set("bar", new Integer(6))
            .set("foo", "booyah")
            .build();
        
        AvroMessage<GenericRecord> msg = new AvroMessage<GenericRecord>(schemaId, origRec);
        
        AvroEncoder encoder = getEncoder();
        final byte[] bytes = encoder.encode(msg);
        
        AvroDecoder<GenericRecord> decoder = new AvroDecoder<GenericRecord>(GenericRecord.class);
        AvroMessage<GenericRecord> decoded = decoder.decode(bytes, origRec.getSchema(), schemaId);
        
        Map<String, String> expectedHdrs = ImmutableMap.of("Content-Type", "application/x-avro-binary", "X-Rico-Schema-Id", schemaId);
        assertEquals(expectedHdrs, decoded.headers());
        assertEquals(origRec, decoded.body());
    }
}
