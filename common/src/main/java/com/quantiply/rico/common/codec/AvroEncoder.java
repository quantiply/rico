package com.quantiply.rico.common.codec;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
/**
 * Encodes Avro messages
 * 
 * Assumptions:
 *  - Messages have a schema id
 * 
 * @author rhoover
 *
 */
public class AvroEncoder<T extends GenericRecord> {

    public static final String AVRO_CONTENT_TYPE = "application/x-avro-binary";
    
    private final EncoderFactory encoderFactory = EncoderFactory.get();
    private org.apache.avro.io.BinaryEncoder avroEncoder = null;
    private Encoder encoder = new Encoder();
    
    public byte[] encode(final AvroMessage<T> msg) throws IOException {
        //TODO - do this in a util?
        msg.headers().put("Content-Type", AVRO_CONTENT_TYPE);
        msg.headers().put("X-Rico-Schema-Id", msg.schemaId());
        
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        
        DatumWriter<T> writer;
        if (msg.body() instanceof SpecificRecord)
            writer = new SpecificDatumWriter<T>(msg.schema());
        else
            writer = new GenericDatumWriter<T>(msg.schema());
        
        avroEncoder = encoderFactory.directBinaryEncoder(out, avroEncoder); 
        writer.write(msg.body(), avroEncoder);
        
        return encoder.encode(out.toByteArray(), msg.headers());
    }
    
}
