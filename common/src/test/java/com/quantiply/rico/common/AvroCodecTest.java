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

import com.quantiply.rico.common.codec.AvroSpecificDecoder;
import com.quantiply.rico.common.codec.AvroEncoder;
import com.quantiply.rico.common.codec.AvroGenericDecoder;
import com.quantiply.rico.common.codec.AvroGenericDecoder.SchemaPair;
import com.quantiply.rico.common.codec.AvroMessage;
import com.quantiply.rico.common.codec.Headers;
import com.quantiply.rico.common.codec.RawMessage;
import com.quantiply.schema.test.Fubar;

public class AvroCodecTest {

    protected Headers getHeaders() {
        DateTime occurred = ISODateTimeFormat.dateTime().parseDateTime("2014-07-23T00:06:00.000Z");
        return new Headers("msgId", occurred, "fakeSchemaId", null);
    }

    protected Schema getV0Schema() {
        return SchemaBuilder
                .record("Fubar").namespace("com.quantiply.schema.test")
                        .fields()
                          .name("foo").type()
                              .stringBuilder()
                                  .prop("avro.java.string", "String")
                              .endString()
                              .noDefault()
                          .name("bar").type("int").noDefault()
                          .name("charlie").type().stringType().noDefault()
                        .endRecord();
    }
    
    //NOTE: v1 schema is in Fubar.avsc
    
    protected Schema getV2Schema() {
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
        
        AvroSpecificDecoder<Fubar> decoder = new AvroSpecificDecoder<Fubar>(Fubar.class);
        AvroMessage<Fubar> decoded = decoder.decode(bytes, new Function<RawMessage, Schema>() {
            @Override
            public Schema call(RawMessage input) throws Exception {
                return origRec.getSchema();
            }
        });
        
        assertEquals(hdrs, decoded.getHeaders());
        assertEquals(origRec, decoded.getBody());
        
    }
    
    @Test
    public void decodeNewVersionSpecific() throws IOException {
        final GenericRecord origRec = new GenericRecordBuilder(getV0Schema())
            .set("foo", "hi")
            .set("bar", new Integer(7))
            .set("charlie", "i won't make it")
            .build();
        
        Headers hdrs = getHeaders();
        AvroMessage<GenericRecord> msg = new AvroMessage<GenericRecord>(origRec, hdrs);
        
        AvroEncoder<GenericRecord> encoder = new AvroEncoder<GenericRecord>();
        final byte[] bytes = encoder.encode(msg);
        
        //Now decode with a newer schema
        AvroSpecificDecoder<Fubar> v2Decoder = new AvroSpecificDecoder<Fubar>(Fubar.class);
        AvroMessage<Fubar> v2Decoded = v2Decoder.decode(bytes, new Function<RawMessage, Schema>() {
            @Override
            public Schema call(RawMessage input) throws Exception {
                //Must return the writer's schema here
                return getV0Schema();
            }
        });
        Fubar f = v2Decoded.getBody();
        assertEquals("hi", f.getFoo());
        assertEquals(7, f.getBar().intValue());
        //Old param is gone - throws NPE
        try {
            f.get("charlie");
            assertTrue(false);
        }
        catch (NullPointerException e) {
            assertTrue(true);
        }
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
        
        final AvroGenericDecoder decoder = new AvroGenericDecoder();
        AvroMessage<GenericRecord> decoded = decoder.decode(bytes, new Function<RawMessage, SchemaPair>() {
            @Override
            public SchemaPair call(RawMessage raw) throws Exception {
                return decoder.new SchemaPair(origRec.getSchema(), null);
            }
        });
        
        assertEquals(origRec, decoded.getBody());
        
        //Now decode with a newer schema that also uses the same record name
        final AvroGenericDecoder v2Decoder = new AvroGenericDecoder();
        AvroMessage<GenericRecord> v2Decoded = v2Decoder.decode(bytes, new Function<RawMessage, SchemaPair>() {
            @Override
            public SchemaPair call(RawMessage raw) throws Exception {
                return decoder.new SchemaPair(origRec.getSchema(), getV2Schema());
            }
        });
        //Check that the new parameter is present with it's default value
        assertEquals(42, ((Integer)v2Decoded.getBody().get("newParam")).intValue());
    }
}
