package com.quantiply.rico.common.codec;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.google.common.collect.ImmutableMap;

import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;

import com.quantiply.schema.WrappedMsg;

public class Decoder {

    private final WrappedMsg wrapped = new WrappedMsg();
    private final DecoderFactory decoderFactory = DecoderFactory.get();
    private org.apache.avro.io.BinaryDecoder decoder = null;
    private final DatumReader<WrappedMsg> reader = new SpecificDatumReader<WrappedMsg>(WrappedMsg.class);
    
    public Message decode(byte[] payload) throws IOException {
        //Check version byte
        ByteBuffer buffer = getByteBuffer(payload);
        
        //Decode wrapped msg
        decoder = decoderFactory.binaryDecoder(buffer.array(), buffer.position(), buffer.remaining(), decoder);
        reader.read(wrapped, decoder);
        
        return new Message(wrapped.getBody().array(), ImmutableMap.copyOf(wrapped.getHeaders()));
    }
    
    private ByteBuffer getByteBuffer(byte[] payload) {
        ByteBuffer buffer = ByteBuffer.wrap(payload);
        byte version = buffer.get();
        if (version != Encoder.MSG_FORMAT_VERSION)
          throw new IllegalArgumentException(String.format("Unknown message version: %x", version));
        return buffer;
      }
    
}
