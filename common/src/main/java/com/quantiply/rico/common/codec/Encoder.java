package com.quantiply.rico.common.codec;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.hadoop.mapreduce.lib.input.InvalidInputException;

import com.quantiply.schema.WrappedMsg;

/**
 * Message encoder
 * 
 * Wraps messages with 
 *   - A version byte to allow for future format changes
 *   - headers to allow for metadata
 * 
 * The headers are string key/value pairs. We want them to be
 * human readable.
 * 
 * Note: we're using Avro to encode the headers for speed/correctness
 *  of implementation.  Later, a different serialization could be used to
 *  minimize dependencies if desired.
 *  
 *  We could even use HTTP header format
 *   http://hc.apache.org/httpclient-3.x/apidocs/org/apache/commons/httpclient/HttpParser.html#parseHeaders(java.io.InputStream,%20java.lang.String)
 * 
 * @author rhoover
 */
public class Encoder {

    protected static final byte MSG_FORMAT_VERSION = 0x1;
    
    private final EncoderFactory encoderFactory = EncoderFactory.get();
    private org.apache.avro.io.BinaryEncoder encoder = null;
    private final DatumWriter<WrappedMsg> writer = new SpecificDatumWriter<WrappedMsg>(WrappedMsg.class);
    
    /**
     * Wraps message
     * 
     * @throws IOException 
     */
    public byte[] encode(final byte[] msg, final Map<String, String> headers) throws IOException {
        if (msg == null) {
            throw new IllegalArgumentException("Null message");
        }
        if (msg.length == 0 && headers == null) {
            throw new IllegalArgumentException("Message has no content");
        }
        
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        
        com.quantiply.schema.WrappedMsg.Builder builder = WrappedMsg.newBuilder();
        builder.setBody(ByteBuffer.wrap(msg));
        if (headers != null) {
            builder.setHeaders(headers);
        }
        
        WrappedMsg wrapped = builder.build();
        
        out.write(MSG_FORMAT_VERSION);
        encoder = encoderFactory.directBinaryEncoder(out, encoder); 
        writer.write(wrapped, encoder);
        return out.toByteArray();
    }
    
    public byte[] encode(final Message msg) throws IOException {
        return encode(msg.body(), msg.headers());
    }
    
    public byte[] encode(final byte[] msg) throws IOException {
        return encode(msg, null);
    }
}
