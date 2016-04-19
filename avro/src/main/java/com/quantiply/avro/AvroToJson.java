/*
 * Copyright 2014-2016 Quantiply Corporation. All rights reserved.
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
    public static abstract class IgnoreTypeMixIn { }

    public static class Utf8Serializer extends JsonSerializer<Utf8> {
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

    public AvroToJson() {
        this(null);
    }

    public AvroToJson(AvroToJSONCustomizer customizer) {
        if (customizer != null) {
            customizer.customize(objMapper);
        }
    }

    public byte[] objectToJson(Object msg) throws Exception {
        return objMapper.writeValueAsBytes(msg);
    }

    public <T> T jsonToObject(byte[] bytes, Class<T> klaz) throws IOException {
        return objMapper.readValue(bytes, klaz);
    }
}
