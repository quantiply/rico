package com.quantiply.rico.common.codec;

import java.io.IOException;

import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;

public class AvroBaseDecoder<T extends IndexedRecord> {
    
    protected final DecoderFactory decoderFactory = DecoderFactory.get();
    protected org.apache.avro.io.BinaryDecoder avroDecoder = null;
    protected RawMessageDecoder decoder = new RawMessageDecoder();
    
    protected AvroMessage<T> decode(RawMessage raw, DatumReader<T> reader) throws IOException {
        avroDecoder = decoderFactory.binaryDecoder(raw.getBody(), avroDecoder);
        T body = reader.read(null, avroDecoder);
        return new AvroMessage<T>(body, raw.getHeaders());
    }

}
