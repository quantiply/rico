package com.quantiply.rico.common.codec;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.EncoderFactory;

import com.quantiply.schema.WrappedMsg;

public class Encoder {

    protected static final byte MSG_FORMAT_VERSION = 0x1;
    
    /**
     * Wraps message with 
     * 1) A version byte to allow for future format changes
     * 2) headers to allow for metadata
     * 
     * @throws IOException 
     */
    public byte[] toBytes(final byte[] msg, final Map<String, String> headers) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        
        /*
         * TODO: Try to reuse as many objects as possible to avoid needlessly creating objects
         */
        WrappedMsg wrapped = WrappedMsg.newBuilder()
                .setHeaders(headers)
                .setBody(ByteBuffer.wrap(msg))
                .build();
        GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(WrappedMsg.getClassSchema());
        
        out.write(MSG_FORMAT_VERSION);
        org.apache.avro.io.Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        writer.write(wrapped, encoder);
        encoder.flush();
        return out.toByteArray();
    }
    
}
