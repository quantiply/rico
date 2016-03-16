package com.quantiply.samza.task;

import com.quantiply.samza.MetricAdaptor;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.samza.Partition;
import org.apache.samza.config.Config;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.task.TaskContext;
import org.junit.Test;

import static org.junit.Assert.*;

public class BaseTaskTest {

    class MyTask extends BaseTask {
        @Override
        protected void _init(Config config, TaskContext context, MetricAdaptor metricAdaptor) throws Exception {
        }
    }

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
    public void getMessageIdFromSource() {
        MyTask myTask = new MyTask();
        String offset = "12345";
        SystemStreamPartition ssp = new SystemStreamPartition("fakeSystem", "fakeStream", new Partition(56789));
        IncomingMessageEnvelope envelope = new IncomingMessageEnvelope(ssp, offset, null, null);
        assertEquals("fakeStream-56789-12345", myTask.getMessageIdFromSource(envelope));
    }

    @Test
    public void getCamusHeaders() {
        MyTask myTask = new MyTask();
        GenericRecord inHdr = getHeader();
        GenericData.Record outHdr = myTask.getCamusHeaders(inHdr, getHeaderSchema(), 21L);
        assertEquals("testId", outHdr.get("id").toString());
        assertEquals(99L, ((Long)outHdr.get("time")).longValue());
        assertEquals(21L, ((Long)outHdr.get("created")).longValue());
    }

    @Test
    public void getCamusHeadersNewToOld() {
        MyTask myTask = new MyTask();
        GenericRecord inHdr = getHeader();
        GenericData.Record outHdr = myTask.getCamusHeaders(inHdr, getOldHeaderSchema(), 21L);
        assertEquals("testId", outHdr.get("id").toString());
        assertEquals(99L, ((Long)outHdr.get("timestamp")).longValue());
        assertEquals(21L, ((Long)outHdr.get("created")).longValue());
    }

    @Test
    public void getCamusHeadersOldToNew() {
        MyTask myTask = new MyTask();
        GenericRecord inHdr = getOldHeader();
        GenericData.Record outHdr = myTask.getCamusHeaders(inHdr, getHeaderSchema(), 21L);
        assertEquals("testId", outHdr.get("id").toString());
        assertEquals(99L, ((Long)outHdr.get("time")).longValue());
        assertEquals(21L, ((Long)outHdr.get("created")).longValue());
    }


}