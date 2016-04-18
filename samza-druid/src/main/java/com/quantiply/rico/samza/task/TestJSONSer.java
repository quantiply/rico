package com.quantiply.rico.samza.task;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

/**
 * Created by sarrawat on 4/18/16.
 */
public class TestJSONSer {

    public static void main(String[] args) throws IOException {
        Queue<Map<String, Object>> q = new LinkedList<>();

        for (int i = 0; i < 10; i++) {
            Map<String, Object> a = new HashMap<>();
            a.put("hi", "there");

            q.add(a);
        }

        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final ObjectMapper mapper = new ObjectMapper();

        mapper.writeValue(out, q);

        final byte[] data = out.toByteArray();
        System.out.println(new String(data));
    }
}
