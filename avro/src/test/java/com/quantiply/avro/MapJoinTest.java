package com.quantiply.avro;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.util.Utf8;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.junit.Assert.*;

public class MapJoinTest {

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
                .name("left_out").type().stringType().noDefault()
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
    public void testJoin() throws IOException {
        GenericRecord in1 = getIn1();
        GenericRecord in2 = getIn2();

        Map<String, Object> joined = new MapJoin(getJoinedSchema())
                .merge(in1)
                .merge(in2)
                .getMap();

        assertEquals("yo yo", joined.get("foo"));
        assertEquals(5, joined.get("bar"));
    }

    private GenericRecord getIn2() {
        return new GenericRecordBuilder(getInput2Schema())
                .set("bar", 5)
                .build();
    }

    private GenericRecord getIn1() {
        return new GenericRecordBuilder(getInput1Schema())
                .set("foo", new Utf8("yo yo".getBytes(StandardCharsets.UTF_8)))
                .set("left_out", "forget me")
                .build();
    }
}