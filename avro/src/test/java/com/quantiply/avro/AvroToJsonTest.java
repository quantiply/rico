package com.quantiply.avro;

import java.util.Map;

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
}