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
public class AvroEncoder {

    private final EncoderFactory encoderFactory = EncoderFactory.get();
    private org.apache.avro.io.BinaryEncoder avroEncoder = null;
    private Encoder encoder = new Encoder();
    
    public byte[] encode(final AvroMessage<? extends GenericRecord> msg) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        
        DatumWriter<GenericRecord> writer;
        if (msg.getBody() instanceof SpecificRecord)
            writer = new SpecificDatumWriter<GenericRecord>(msg.getSchema());
        else
            writer = new GenericDatumWriter<GenericRecord>(msg.getSchema());
        
        avroEncoder = encoderFactory.directBinaryEncoder(out, avroEncoder); 
        writer.write(msg.getBody(), avroEncoder);
        
        return encoder.encode(out.toByteArray(), msg.getHeaders());
    }
    
}
