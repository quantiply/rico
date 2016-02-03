package com.quantiply.avro;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreType;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.apache.avro.Schema;
import org.apache.avro.util.Utf8;

import java.io.IOException;

public class AvroToJson {
    private final ObjectMapper objMapper;

    @JsonIgnoreType
    abstract class IgnoreTypeMixIn { }

    public class Utf8Serializer extends JsonSerializer<Utf8> {
        @Override
        public void serialize(Utf8 value, JsonGenerator jgen, SerializerProvider provider)
                throws IOException {
            jgen.writeString(value.toString());
        }
    }

    {
        objMapper = new ObjectMapper();
        //Do not serialize Avro schemas
        objMapper.addMixInAnnotations(Schema.class, IgnoreTypeMixIn.class);
        //Use only public getters
        objMapper.setVisibility(PropertyAccessor.ALL, JsonAutoDetect.Visibility.NONE);
        objMapper.setVisibility(PropertyAccessor.GETTER, JsonAutoDetect.Visibility.PUBLIC_ONLY);
        //Use lower case with underscores for JSON field names
        objMapper.setPropertyNamingStrategy(PropertyNamingStrategy.CAMEL_CASE_TO_LOWER_CASE_WITH_UNDERSCORES);

        //Convert Avro Utf8 CharSequences to Strings
        SimpleModule module = new SimpleModule();
        module.addSerializer(Utf8.class, new Utf8Serializer());
        objMapper.registerModule(module);
    }

    public byte[] objectToJson(Object msg) throws Exception {
        return objMapper.writeValueAsBytes(msg);
    }

    public <T> T jsonToObject(byte[] bytes, Class<T> klaz) throws IOException {
        return objMapper.readValue(bytes, klaz);
    }
}
