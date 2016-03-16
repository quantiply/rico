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

import java.util.Map;

import com.quantiply.test.User;
import org.apache.avro.util.Utf8;
import org.junit.Test;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.nio.charset.StandardCharsets;

import static org.junit.Assert.*;

public class AvroToJsonTest {

    class Thing {

        public String getFoo() {
            return "hi";
        }

        public CharSequence getBar() {
            return new Utf8("hello".getBytes(StandardCharsets.UTF_8));
        }

    }

    @Test
    public void testObjectToJson() throws Exception {
        AvroToJson avroToJson = new AvroToJson();
        Thing thing = new Thing();
        ObjectMapper objectMapper = new ObjectMapper();
        byte[] jsonBytes = avroToJson.objectToJson(thing);
        Map<String,String> obj = objectMapper.readValue(jsonBytes, Map.class);
        assertEquals("hi", obj.get("foo"));
        assertEquals("hello", obj.get("bar"));
        String jsonStr = new String(jsonBytes, StandardCharsets.UTF_8);
        assertTrue(jsonStr.contains("\"bar\":\"hello\""));
    }

    @Test
    public void testJsonToObject() throws Exception {
        AvroToJson avroToJson = new AvroToJson();
        User expectedUser = User.newBuilder().setAge(5).setName("Bumpkin").build();
        String jsonStr = "{\"name\":\"Bumpkin\",\"age\":5}";
        User user = avroToJson.jsonToObject(jsonStr.getBytes(StandardCharsets.UTF_8), User.class);
        assertEquals(expectedUser, user);
    }
}
