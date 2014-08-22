package com.quantiply.rico.common.codec;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import com.quantiply.schema.WrappedMsg;

/**
 * Raw message encoder
 * 
 * Wraps messages with 
 *   - A version byte to allow for future format changes
 *   - headers to allow for metadata (see the WrappedMsg schema: WrappedMsg.avsc)
 * 
 * Note: we're using Avro to encode the headers for speed/correctness
 *  of implementation, compactness, and ability to evolve. Later, a 
 *  different serialization could be used to minimize dependencies if desired.
 *  
 * @author rhoover
 */
public class RawMessageEncoder {

    protected static final byte MSG_FORMAT_VERSION = 0x1;
    
    private final EncoderFactory encoderFactory = EncoderFactory.get();
    private org.apache.avro.io.BinaryEncoder encoder = null;
    private final DatumWriter<WrappedMsg> writer = new SpecificDatumWriter<WrappedMsg>(WrappedMsg.class);
    private final DateTimeFormatter dateFormatter = ISODateTimeFormat.dateTime().withZone(DateTimeZone.UTC);
    
    /**
     * Wraps message
     * 
     * @throws IOException 
     */
    public byte[] encode(final byte[] msg, final Headers headers) throws IOException {
        if (msg == null || msg.length == 0) {
            throw new IllegalArgumentException("No message");
        }
        if (headers == null) {
            throw new IllegalArgumentException("Null headers");
        }
        
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        WrappedMsg wrapped = WrappedMsg.newBuilder()
            .setHeaders(getAvroHeaders(headers))
            .setBody(ByteBuffer.wrap(msg))
            .build();
        
        out.write(MSG_FORMAT_VERSION);
        encoder = encoderFactory.directBinaryEncoder(out, encoder); 
        writer.write(wrapped, encoder);
        return out.toByteArray();
    }

    protected com.quantiply.schema.Headers getAvroHeaders(final Headers headers) {
        String occurredStr = dateFormatter.print(headers.getOccured());
        com.quantiply.schema.Headers.Builder builder = com.quantiply.schema.Headers.newBuilder();
        builder.setId(headers.getId())
            .setOccurred(occurredStr)
            .setKv(headers.getKv());
        if (headers.getSchemaId() != null) {
            builder.setSchemaId(headers.getSchemaId());
        }
        return builder.build();
    }
    
    public byte[] encode(final RawMessage msg) throws IOException {
        return encode(msg.getBody(), msg.getHeaders());
    }
    
}
