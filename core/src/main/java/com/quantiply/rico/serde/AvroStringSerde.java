package com.quantiply.rico.serde;

import com.quantiply.rico.Configuration;
import com.quantiply.rico.Envelope;
import com.quantiply.rico.errors.ConfigException;
import com.quantiply.rico.errors.SerializationException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.JsonDecoder;
import org.apache.avro.specific.SpecificRecord;

import java.io.IOException;
import java.io.StringWriter;

/**
 * Created by rhoover on 3/21/15.
 */
public class AvroStringSerde implements StringSerde<Object> {
    private Schema _schema;
    private DatumReader<Object> _reader;
    private JsonDecoder _decoder;

    @Override
    public void init(Configuration cfg) throws ConfigException {
        //get a reference to the Avro Specific Record class here
        String recordClassName = cfg.getString("string-serde.avro.type");
        try {
            Class clazz = Class.forName(recordClassName);
            SpecificRecord record = (SpecificRecord) clazz.newInstance();
            _schema = record.getSchema();
            _reader = new GenericDatumReader<Object>(_schema);
            _decoder = DecoderFactory.get().jsonDecoder(_schema, "");
        }
        catch (Exception e) {
            throw new ConfigException(e);
        }
    }

    @Override
    public Envelope<Object> fromString(String jsonMsg) throws SerializationException {
        Envelope<Object> envelope = new Envelope<>();

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
    public void writeTo(Envelope<Object> envelope, StringWriter writer) throws SerializationException {

    }
}
