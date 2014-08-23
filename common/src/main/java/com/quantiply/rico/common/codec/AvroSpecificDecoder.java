package com.quantiply.rico.common.codec;

import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecord;

import com.quantiply.rico.common.Function;

public class AvroSpecificDecoder<T extends SpecificRecord> extends AvroBaseDecoder<T>{
    private final Class<T> typeClass;
    
    public AvroSpecificDecoder(Class<T> typeClass) {
        this.typeClass = typeClass;
    }
    
    public AvroMessage<T> decode(byte[] payload, Function<RawMessage, Schema> rawToWritersSchema) throws IOException {
        final RawMessage raw = decoder.decode(payload);
        final Schema writersSchema;
        try {
            writersSchema = rawToWritersSchema.call(raw);
        }
        catch (Exception e) {
            throw new IOException(e);
        }
        final Schema readersSchema;
        try {
            readersSchema = (Schema) typeClass.getMethod("getClassSchema").invoke(null);
        }
        catch (Exception e) {
            throw new IOException(e);
        }
        DatumReader<T> reader = new SpecificDatumReader<T>(writersSchema, readersSchema);
        return decode(raw, reader);
    }
    
}
