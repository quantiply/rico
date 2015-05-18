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
package com.quantiply.avro;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class JoinTest {

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

        GenericRecord joined = new Join(getJoinedSchema())
                .merge(in1)
                .merge(in2)
                .getBuilder()
                .set("charlie", "blah blah")
                .build();

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
                .set("left_out", "forget me")
                .build();
    }

}