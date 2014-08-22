package com.quantiply.rico.common.codec;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import com.quantiply.schema.WrappedMsg;

public class Decoder {

    private final WrappedMsg wrapped = new WrappedMsg();
    private final DecoderFactory decoderFactory = DecoderFactory.get();
    private org.apache.avro.io.BinaryDecoder decoder = null;
    private final DatumReader<WrappedMsg> reader = new SpecificDatumReader<WrappedMsg>(WrappedMsg.class);
    private final DateTimeFormatter dateFormatter = ISODateTimeFormat.dateTime();
    
    public RawMessage decode(byte[] payload) throws IOException {
        //Check version byte
        ByteBuffer buffer = getByteBuffer(payload);
        
        //Decode wrapped msg
        decoder = decoderFactory.binaryDecoder(buffer.array(), buffer.position(), buffer.remaining(), decoder);
        reader.read(wrapped, decoder);
        
        return new RawMessage(wrapped.getBody().array(), getHeaders(wrapped.getHeaders()));
    }
    
    protected Headers getHeaders(com.quantiply.schema.Headers avroHeaders) {
        DateTime occurred = dateFormatter.parseDateTime(avroHeaders.getOccurred());
        return new Headers(avroHeaders.getId(), occurred, avroHeaders.getSchemaId(), avroHeaders.getKv());
    }
    
    private ByteBuffer getByteBuffer(byte[] payload) {
        ByteBuffer buffer = ByteBuffer.wrap(payload);
        byte version = buffer.get();
        if (version != Encoder.MSG_FORMAT_VERSION)
          throw new IllegalArgumentException(String.format("Unknown message version: %x", version));
        return buffer;
      }
    
}
