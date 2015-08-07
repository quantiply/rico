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
