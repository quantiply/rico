package com.quantiply.rico.common.codec;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

public class AvroMessage<T extends GenericRecord> extends SchemaMessage<T, Schema> {

    public AvroMessage(T body, Headers headers) {
        super(body.getSchema(), body, headers);
    }

}
