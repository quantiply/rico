package com.quantiply.avro;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreType;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.deser.BeanDeserializerModifier;
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

        SimpleModule module = new SimpleModule();
        //Convert Avro Utf8 CharSequences to Strings
        module.addSerializer(Utf8.class, new Utf8Serializer());

        //Allow lower case ENUM strings
        //http://stackoverflow.com/questions/24157817/jackson-databind-enum-case-insensitive
        module.setDeserializerModifier(new BeanDeserializerModifier() {
            @Override
            public JsonDeserializer<Enum> modifyEnumDeserializer(DeserializationConfig config,
                                                                 final JavaType type,
                                                                 BeanDescription beanDesc,
                                                                 final JsonDeserializer<?> deserializer) {
                return new JsonDeserializer<Enum>() {
                    @Override
                    public Enum deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException {
                        Class<? extends Enum> rawClass = (Class<Enum<?>>) type.getRawClass();
                        return Enum.valueOf(rawClass, jp.getValueAsString().toUpperCase());
                    }
                };
            }
        });

        objMapper.registerModule(module);
    }

    public byte[] objectToJson(Object msg) throws Exception {
        return objMapper.writeValueAsBytes(msg);
    }

    public <T> T jsonToObject(byte[] bytes, Class<T> klaz) throws IOException {
        return objMapper.readValue(bytes, klaz);
    }
}
