package com.quantiply.samza;

import com.quantiply.samza.serde.JsonSerde;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class JsonSerdeTest {

    @Test
    public void testSerde() {
        JsonSerde jsonSerde = new JsonSerde();
        String jsonStr = "{\"hi\":123}";
        Map<String, Object> parsed = (Map<String, Object>) jsonSerde.fromBytes(jsonStr.getBytes(StandardCharsets.UTF_8));
        assertEquals(123, ((Number)parsed.get("hi")).intValue());
        String toStr = jsonSerde.toString(parsed);
        assertEquals(jsonStr, toStr);
        byte[] bytes = jsonSerde.toBytes(parsed);
        assertArrayEquals(jsonStr.getBytes(StandardCharsets.UTF_8), bytes);
    }

}