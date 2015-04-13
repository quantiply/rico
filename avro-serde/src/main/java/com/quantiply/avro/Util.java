package com.quantiply.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;

public class Util {

    public static void merge(GenericRecord merged, GenericRecord input) {
        Schema outSchema = merged.getSchema();
        for (Schema.Field field : input.getSchema().getFields()) {
            Schema.Field outField = outSchema.getField(field.name());
            if (outField != null) {
                merged.put(outField.pos(), input.get(field.pos()));
            }
        }
    }

    public static void merge(GenericRecordBuilder builder, Schema outSchema, GenericRecord input) {
        for (Schema.Field field : input.getSchema().getFields()) {
            Schema.Field outField = outSchema.getField(field.name());
            if (outField != null) {
                builder.set(outField, input.get(field.pos()));
            }
        }
    }

}
