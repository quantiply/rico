package com.quantiply.avro;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static org.junit.Assert.*;

public class UtilTest {

    protected Schema getJoinedSchema() {
        return SchemaBuilder
                .record("Joined").namespace("com.quantiply.test")
                .fields()
                .name("foo").type().stringType().noDefault()
                .name("bar").type("int").noDefault()
                .name("charlie").type().stringType().noDefault()
                .endRecord();
    }

    protected Schema getInput1Schema() {
        return SchemaBuilder
                .record("In1").namespace("com.quantiply.test")
                .fields()
                .name("foo").type().stringType().noDefault()
                .endRecord();
    }

    protected Schema getInput2Schema() {
        return SchemaBuilder
                .record("In1").namespace("com.quantiply.test")
                .fields()
                .name("bar").type("int").noDefault()
                .endRecord();
    }

    @Test
    public void testMerge() throws IOException {
        GenericRecord in1 = getIn1();
        GenericRecord in2 = getIn2();

        GenericData.Record joined = new GenericData.Record(getJoinedSchema());
        Util.merge(joined, in1);
        Util.merge(joined, in2);

        assertEquals("yo yo", joined.get("foo"));
        assertEquals(5, joined.get("bar"));
        assertNull(joined.get("charlie"));
    }

    @Test
    public void testMergeWithBuilder() throws IOException {
        GenericRecord in1 = getIn1();
        GenericRecord in2 = getIn2();

        GenericRecordBuilder builder = new GenericRecordBuilder(getJoinedSchema());
        Util.merge(builder, getJoinedSchema(), in1);
        Util.merge(builder, getJoinedSchema(), in2);
        GenericRecord joined = builder
                .set("charlie", "blah blah").build();

        assertEquals("yo yo", joined.get("foo"));
        assertEquals(5, joined.get("bar"));
        assertEquals("blah blah", joined.get("charlie"));
    }

    private GenericRecord getIn2() {
        return new GenericRecordBuilder(getInput2Schema())
                    .set("bar", 5)
                    .build();
    }

    private GenericRecord getIn1() {
        return new GenericRecordBuilder(getInput1Schema())
                    .set("foo", "yo yo")
                    .build();
    }

}