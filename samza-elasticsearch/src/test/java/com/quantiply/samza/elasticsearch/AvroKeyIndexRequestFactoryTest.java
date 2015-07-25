package com.quantiply.samza.elasticsearch;

import com.quantiply.rico.elasticsearch.IndexRequestKey;
import com.quantiply.rico.elasticsearch.VersionType;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.elasticsearch.action.index.IndexRequest;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.Assert.*;

public class AvroKeyIndexRequestFactoryTest {

    @Test
    public void testAvroKey() {
        AvroKeyIndexRequestFactory factory = new AvroKeyIndexRequestFactory();
        IndexRequestKey key = IndexRequestKey.newBuilder()
                .setId("fakeid")
                .setVersion(450L)
                .setVersionType(VersionType.EXTERNAL)
                .build();
        OutgoingMessageEnvelope msg = new OutgoingMessageEnvelope(new SystemStream("es", "test_index/test_type"), null, key, "{}".getBytes(StandardCharsets.UTF_8));
        IndexRequest request = factory.getIndexRequest(msg);
        assertEquals("test_index", request.index());
        assertEquals("test_type", request.type());
        assertEquals("fakeid", request.id());
        assertEquals(450L, request.version());
        assertEquals(org.elasticsearch.index.VersionType.EXTERNAL, request.versionType());
    }

}