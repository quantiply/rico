package com.quantiply.rico.common.codec;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecord;

public class AvroDecoder<T extends GenericRecord> {
    private final DecoderFactory decoderFactory = DecoderFactory.get();
    private org.apache.avro.io.BinaryDecoder avroDecoder = null;
    private Decoder decoder = new Decoder();
    
    private final Class<T> typeClass;

    public AvroDecoder(Class<T> typeClass) {
        this.typeClass = typeClass;
    }
    
    public AvroMessage<T> decode(byte[] payload, Schema schema) throws IOException {
        RawMessage raw = decoder.decode(payload);
        
        avroDecoder = decoderFactory.binaryDecoder(raw.body(), avroDecoder);
        DatumReader<T> reader;
        if (SpecificRecord.class.isAssignableFrom(typeClass)) {
            reader = new SpecificDatumReader<T>(typeClass);
        }
        else {
            reader = new GenericDatumReader<T>(schema);
        }
        T body = reader.read(null, avroDecoder);
        
        return new AvroMessage<T>(body, raw.headers());
    }
    
}
