package com.quantiply.samza.serde;

import org.apache.samza.SamzaException;
import org.apache.samza.serializers.Serde;
import org.apache.samza.serializers.model.SamzaObjectMapper;
import org.codehaus.jackson.map.ObjectMapper;
import java.nio.charset.StandardCharsets;

/**
 * We need our own JSON serde because sometimes we need String
 * instead of bytes (e.g. using JEST client for Elasticsearch)
 */
public class JsonSerde implements Serde<Object> {
    protected ObjectMapper mapper = SamzaObjectMapper.getObjectMapper();

    @Override
    public Object fromBytes(byte[] bytes) {
        try {
            return mapper.readValue(new String(bytes, StandardCharsets.UTF_8), Object.class);
        }
        catch (Exception e) {
            throw new SamzaException(e);
        }
    }

    @Override
    public byte[] toBytes(Object o) {
        return toString(o).getBytes(StandardCharsets.UTF_8);
    }

    public String toString(Object o) {
        try {
            return mapper.writeValueAsString(o);
        }
        catch (Exception e) {
           throw new SamzaException(e);
        }
    }
}
