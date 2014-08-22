package com.quantiply.rico.common.codec;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecord;

import com.quantiply.rico.common.Function;

public class AvroDecoder<T extends GenericRecord> {
    private final Class<T> typeClass;
    private final DecoderFactory decoderFactory = DecoderFactory.get();
    private org.apache.avro.io.BinaryDecoder avroDecoder = null;
    private RawMessageDecoder decoder = new RawMessageDecoder();
    
    public AvroDecoder(Class<T> typeClass) {
        this.typeClass = typeClass;
    }
    
    /**
     * 
     * @param payload raw message bytes
     * @param rawToSchema a function to lookup the schema for a raw message
     * @return  the decoded message
     * @throws IOException
     */
    public AvroMessage<T> decode(byte[] payload, Function<RawMessage, Schema> rawToSchema) throws IOException {
        RawMessage raw = decoder.decode(payload);
        Schema schema;
        try {
            schema = rawToSchema.call(raw);
        }
        catch (Exception e) {
            throw new IOException(e);
        }
        DatumReader<T> reader;
        if (SpecificRecord.class.isAssignableFrom(typeClass)) {
            reader = new SpecificDatumReader<T>(typeClass);
        }
        else {
            reader = new GenericDatumReader<T>(schema);
        }
        return decode(raw, reader);
    }
    
    protected AvroMessage<T> decode(RawMessage raw, DatumReader<T> reader) throws IOException {
        avroDecoder = decoderFactory.binaryDecoder(raw.getBody(), avroDecoder);
        T body = reader.read(null, avroDecoder);
        return new AvroMessage<T>(body, raw.getHeaders());
    }
}
