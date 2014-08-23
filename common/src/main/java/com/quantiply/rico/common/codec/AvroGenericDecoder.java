package com.quantiply.rico.common.codec;

import java.io.IOException;
import java.util.AbstractMap.SimpleEntry;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;

import com.quantiply.rico.common.Function;

public class AvroGenericDecoder extends AvroBaseDecoder<GenericRecord> {
    
    @SuppressWarnings("serial")
    public class SchemaPair extends SimpleEntry<Schema, Schema> {
        
        public SchemaPair(Schema writer, Schema reader) {
            super(writer, reader);
        }
        
        public Schema getWriter() {
            return this.getKey();
        }
        
        public Schema getReader() {
            return this.getValue();
        }
    }
    
    public AvroMessage<GenericRecord> decode(byte[] payload, Function<RawMessage, SchemaPair> rawToSchemas) throws IOException {
        RawMessage raw = decoder.decode(payload);
        final SchemaPair schemas;
        try {
             schemas = rawToSchemas.call(raw);
        }
        catch (Exception e) {
            throw new IOException(e);
        }
        final DatumReader<GenericRecord> reader;
        if (schemas.getReader() != null) {
            reader = new GenericDatumReader<GenericRecord>(schemas.getWriter(), schemas.getReader());
        }
        else {
            reader = new GenericDatumReader<GenericRecord>(schemas.getWriter());
        }
        return decode(raw, reader);
    }

}
