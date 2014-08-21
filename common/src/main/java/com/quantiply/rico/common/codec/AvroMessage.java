package com.quantiply.rico.common.codec;

import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

public class AvroMessage<T extends GenericRecord> extends SchemaMessage<T, Schema> {

    public AvroMessage(String schemaId, T body,
            Map<String, String> headers) {
        super(body.getSchema(), schemaId, body, headers);
    }

    public AvroMessage(String schemaId, T body) {
        this(schemaId, body, null);
    }

}
