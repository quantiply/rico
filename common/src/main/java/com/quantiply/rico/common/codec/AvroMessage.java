package com.quantiply.rico.common.codec;

import java.util.Map;

import org.apache.avro.Schema;

public class AvroMessage<T> extends SchemaMessage<T, Schema> {

    public AvroMessage(Schema schema, String schemaId, T body,
            Map<String, String> headers) {
        super(schema, schemaId, body, headers);
    }

    public AvroMessage(Schema schema, String schemaId, T body) {
        super(schema, schemaId, body, null);
    }

}
