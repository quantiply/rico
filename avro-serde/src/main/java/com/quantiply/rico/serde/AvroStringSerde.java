package com.quantiply.rico.serde;

import com.quantiply.rico.Configuration;
import com.quantiply.rico.Envelope;
import com.quantiply.rico.errors.ConfigException;
import com.quantiply.rico.errors.SerializationException;
import org.apache.avro.Schema;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonDecoder;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.StringWriter;

/**
 * Created by rhoover on 3/21/15.
 */
public class AvroStringSerde implements StringSerde {
    public final static String CONFIG_INPUT_TYPE = "string-serde.avro.input.type";
    private Schema _inSchema;
    private SpecificDatumReader<Object> _reader;
    private SpecificDatumWriter<Object> _writer;
    private JsonDecoder _decoder;

    @Override
    public void init(Configuration cfg) throws ConfigException {
        //get a reference to the Avro Specific Record class here
        String recordClassName = cfg.getString(CONFIG_INPUT_TYPE);
        if (recordClassName == null) {
            throw new ConfigException("Avro input type not configured. Missing property: " + CONFIG_INPUT_TYPE);
        }
        try {
            Class clazz = Class.forName(recordClassName);
            SpecificRecord record = (SpecificRecord) clazz.newInstance();
            _inSchema = record.getSchema();
            _reader = new SpecificDatumReader<>(_inSchema);
            _writer = new SpecificDatumWriter<>(_inSchema);
            _decoder = DecoderFactory.get().jsonDecoder(_inSchema, "");
        }
        catch (Exception e) {
            throw new ConfigException(e);
        }
    }

    @Override
    public Envelope fromString(String jsonMsg) throws SerializationException {
        Envelope envelope = new Envelope();

        try {
            _decoder.configure(jsonMsg);
            Object object = _reader.read(null, _decoder);
            envelope.setBody(object);
        }
        catch (IOException e) {
            throw new SerializationException(e);
        }
        return envelope;
    }

    @Override
    public void writeTo(Envelope envelope, StringWriter writer) throws SerializationException {
        SpecificRecord msg = (SpecificRecord) envelope.getBody();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        _writer.setSchema(msg.getSchema());
        try {
            JsonEncoder encoder = EncoderFactory.get().jsonEncoder(msg.getSchema(), out);
            _writer.write(msg, encoder);
            encoder.flush();
            writer.write(out.toString("UTF-8"));
        }
        catch (IOException e) {
            throw new SerializationException(e);
        }
    }

}
