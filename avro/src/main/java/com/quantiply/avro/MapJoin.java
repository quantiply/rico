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
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;

import java.util.HashMap;
import java.util.Map;

public class MapJoin {
    private final Schema schema;
    private final Map<String, Object> map = new HashMap<>();

    public MapJoin(Schema schema) {
        this.schema = schema;
    }

    public MapJoin merge(GenericRecord record) {
        putFields(record, map);
        return this;
    }

    private void putFields(GenericRecord record, Map<String, Object> map) {
        for (Schema.Field field : record.getSchema().getFields()) {
            Schema.Field outField = schema.getField(field.name());
            if (outField != null) {
                Object recVal = record.get(field.pos());
                Object val = recVal;
                if (recVal instanceof GenericRecord) {
                    Map<String, Object> fieldMap = new HashMap<>();
                    putFields((GenericRecord)recVal, fieldMap);
                    val = fieldMap;
                }
                //Avro defaults to Utf8 for strings but this is not what we want in the map
                if (val instanceof Utf8) {
                    val = val.toString();
                }
                map.put(outField.name(), val);
            }
        }
    }

    public Map<String, Object> getMap() {
        return map;
    }
}
