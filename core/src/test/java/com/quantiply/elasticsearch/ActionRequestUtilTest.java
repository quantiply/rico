package com.quantiply.elasticsearch;

import com.quantiply.rico.elasticsearch.Action;
import com.quantiply.rico.elasticsearch.ActionRequestKey;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.junit.Test;

import static org.junit.Assert.*;

public class ActionRequestUtilTest {

    protected Schema getOldHeaderSchema() {
        return SchemaBuilder
                .record("Header").namespace("com.quantiply.test")
                .fields()
                .name("id").type().stringType().noDefault()
                .name("timestamp").type().longType().noDefault()
                .name("created").type().longType().noDefault()
                .endRecord();
    }

    protected Schema getHeaderSchema() {
        return SchemaBuilder
                .record("Header").namespace("com.quantiply.test")
                .fields()
                .name("id").type().stringType().noDefault()
                .name("time").type().longType().noDefault()
                .name("created").type().longType().noDefault()
                .endRecord();
    }

    protected GenericRecord getOldHeader() {
        return new GenericRecordBuilder(getOldHeaderSchema())
                .set("id", "testId")
                .set("timestamp", 99L)
                .set("created", 66L)
                .build();
    }

    protected GenericRecord getHeader() {
        return new GenericRecordBuilder(getHeaderSchema())
                .set("id", "testId")
                .set("time", 99L)
                .set("created", 66L)
                .build();
    }

    @Test
    public void testGetIndexRequestKeyFromCamusHeader() throws Exception {
        ActionRequestKey requestKey = ActionRequestUtil.getIndexRequestFromCamusHeader(getHeader(), getHeaderSchema());
        assertEquals(requestKey.getAction(), Action.INDEX);
        assertEquals(99L, requestKey.getPartitionTsUnixMs().longValue());
        assertEquals(99L, requestKey.getEventTsUnixMs().longValue());
        assertEquals("testId", requestKey.getId().toString());
    }

    @Test
    public void testGetIndexRequestKeyFromOldCamusHeader() throws Exception {
        ActionRequestKey requestKey = ActionRequestUtil.getIndexRequestFromCamusHeader(getOldHeader(), getOldHeaderSchema());
        assertEquals(requestKey.getAction(), Action.INDEX);
        assertEquals(99L, requestKey.getPartitionTsUnixMs().longValue());
        assertEquals(99L, requestKey.getEventTsUnixMs().longValue());
        assertEquals("testId", requestKey.getId().toString());
    }
}