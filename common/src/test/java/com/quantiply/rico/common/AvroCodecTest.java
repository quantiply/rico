package com.quantiply.rico.common;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.joda.time.DateTime;
import org.joda.time.format.ISODateTimeFormat;
import org.junit.*;

import static org.junit.Assert.*;

import com.quantiply.rico.common.codec.AvroDecoder;
import com.quantiply.rico.common.codec.AvroEncoder;
import com.quantiply.rico.common.codec.AvroMessage;
import com.quantiply.rico.common.codec.Headers;
import com.quantiply.rico.common.codec.RawMessage;
import com.quantiply.schema.test.Fubar;
import com.quantiply.schema.test.Fubar2;

public class AvroCodecTest {

    protected Headers getHeaders() {
        DateTime occurred = ISODateTimeFormat.dateTime().parseDateTime("2014-07-23T00:06:00.000Z");
        return new Headers("msgId", occurred, "fakeSchemaId", null);
    }
    
    protected Schema getV2SchemaWithConflictingName() {
        return SchemaBuilder
                .record("Fubar").namespace("com.quantiply.schema.test")
                        .fields()
                          .name("foo").type("string").noDefault()
                          .name("bar").type("int").noDefault()
                          .name("newParam").type("int").withDefault(new Integer(42))
                        .endRecord();
    }
    
    @Test
    public void codecSpecificRecord() throws IOException {
        final Fubar origRec = Fubar.newBuilder().setBar(7).setFoo("hi").build();
        
        Headers hdrs = getHeaders();
        AvroMessage<Fubar> msg = new AvroMessage<Fubar>(origRec, hdrs);
        
        AvroEncoder<Fubar> encoder = new AvroEncoder<Fubar>();
        final byte[] bytes = encoder.encode(msg);
        
        AvroDecoder<Fubar> decoder = new AvroDecoder<Fubar>(Fubar.class);
        AvroMessage<Fubar> decoded = decoder.decode(bytes, new Function<RawMessage, Schema>() {
            @Override
            public Schema call(RawMessage input) throws Exception {
                return origRec.getSchema();
            }
        });
        
        assertEquals(hdrs, decoded.getHeaders());
        assertEquals(origRec, decoded.getBody());
        
        //Now decode with a newer schema
        AvroDecoder<Fubar2> v2Decoder = new AvroDecoder<Fubar2>(Fubar2.class);
        AvroMessage<Fubar2> v2Decoded = v2Decoder.decode(bytes, new Function<RawMessage, Schema>() {
            @Override
            public Schema call(RawMessage input) throws Exception {
                return Fubar2.getClassSchema();
            }
        });
        //Check that the new parameter is present with it's default value
        assertEquals(42, v2Decoded.getBody().getNewParam().intValue());
    }
    
    @Test
    public void codecGenericRecord() throws IOException {
        final GenericRecord origRec = new GenericRecordBuilder(Fubar.getClassSchema())
            .set("bar", new Integer(6))
            .set("foo", "booyah")
            .build();
        
        AvroMessage<GenericRecord> msg = new AvroMessage<GenericRecord>(origRec, getHeaders());
        
        AvroEncoder<GenericRecord> encoder = new AvroEncoder<GenericRecord>();
        final byte[] bytes = encoder.encode(msg);
        
        AvroDecoder<GenericRecord> decoder = new AvroDecoder<GenericRecord>(GenericRecord.class);
        AvroMessage<GenericRecord> decoded = decoder.decode(bytes, new Function<RawMessage, Schema>() {
            @Override
            public Schema call(RawMessage input) throws Exception {
                return origRec.getSchema();
            }
        });
        
        assertEquals(origRec, decoded.getBody());
        
        //Now decode with a newer schema that also uses the same record name
        AvroDecoder<GenericRecord> v2Decoder = new AvroDecoder<GenericRecord>(GenericRecord.class);
        AvroMessage<GenericRecord> v2Decoded = v2Decoder.decode(bytes, new Function<RawMessage, Schema>() {
            @Override
            public Schema call(RawMessage input) throws Exception {
                return getV2SchemaWithConflictingName();
            }
        });
        //Check that the new parameter is present with it's default value
        assertEquals(42, ((Integer)v2Decoded.getBody().get("newParam")).intValue());
    }
}
