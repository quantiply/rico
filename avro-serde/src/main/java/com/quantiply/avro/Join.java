package com.quantiply.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;

public class Join {
    private GenericRecordBuilder builder;
    private Schema schema;

    public Join(Schema schema) {
        this.schema = schema;
        this.builder = new GenericRecordBuilder(schema);
    }

    public GenericRecordBuilder getBuilder() {
        return builder;
    }

    public Join merge(GenericRecord record) {
        for (Schema.Field field : record.getSchema().getFields()) {
            Schema.Field outField = schema.getField(field.name());
            if (outField != null) {
                builder.set(outField, record.get(field.pos()));
            }
        }
        return this;
    }
}
