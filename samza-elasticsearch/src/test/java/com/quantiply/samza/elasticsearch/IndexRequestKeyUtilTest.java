/*
 * Copyright 2014-2015 Quantiply Corporation. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.quantiply.samza.elasticsearch;

import com.quantiply.rico.elasticsearch.IndexRequestKey;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.junit.Test;

import static org.junit.Assert.*;

public class IndexRequestKeyUtilTest {

    protected Schema getCamusSchema() {
        return SchemaBuilder
                .record("In1").namespace("com.quantiply.test")
                .fields()
                .name("header").type()
                   .record("Headers").namespace("com.quantiply.test").fields()
                   .name("id").type().unionOf().stringType().and().nullType().endUnion().noDefault()
                   .name("timestamp").type().longType().noDefault()
                   .endRecord().noDefault()
                .endRecord();
    }

    protected GenericRecord getCamusMsg(String id) {
        return new GenericRecordBuilder(getCamusSchema())
                .set("header",
                        new GenericRecordBuilder(getCamusSchema().getField("header").schema())
                                .set("timestamp", 999999L)
                                .set("id", id)
                                .build()
                )
                .build();
    }

    @Test
    public void testGetIndexRequestKeyFromCamus() throws Exception {
        GenericRecord msg = getCamusMsg("fakeId");
        IndexRequestKey key = IndexRequestKeyUtil.getIndexRequestKeyFromCamus(msg);
        assertEquals(999999L, key.getTimestampUnixMs().longValue());
        assertEquals("fakeId", key.getId());
    }

    @Test
    public void testGetIndexRequestKeyFromCamusNullId() throws Exception {
        GenericRecord msg = getCamusMsg(null);
        IndexRequestKey key = IndexRequestKeyUtil.getIndexRequestKeyFromCamus(msg);
        assertEquals(999999L, key.getTimestampUnixMs().longValue());
        assertNull(key.getId());
    }

}