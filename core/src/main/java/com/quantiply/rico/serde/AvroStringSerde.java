package com.quantiply.rico.serde;

import com.quantiply.rico.Configuration;
import com.quantiply.rico.Envelope;
import com.quantiply.rico.errors.ConfigException;
import com.quantiply.rico.errors.SerializationException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificRecord;

import java.io.IOException;
import java.io.StringWriter;

/**
 * Created by rhoover on 3/21/15.
 */
public class AvroStringSerde implements StringSerde<Object> {
    private final DecoderFactory decoderFactory = DecoderFactory.get();
    private Schema _schema;
    private DatumReader<Object> _reader;

    @Override
    public void init(Configuration cfg) throws ConfigException {
        //get a reference to the Avro Specific Record class here
        String recordClassName = cfg.getString("string-serde.avro.type");
        try {
            Class clazz = Class.forName(recordClassName);
            SpecificRecord record = (SpecificRecord) clazz.newInstance();
            _schema = record.getSchema();
        }
        catch (Exception e) {
            throw new ConfigException(e);
        }
        _reader = new GenericDatumReader<Object>(_schema);
    }

    @Override
    public Envelope<Object> fromString(String jsonMsg) throws SerializationException {
        Envelope<Object> envelope = new Envelope<>();

        try {
            Object object = _reader.read(null, decoderFactory.jsonDecoder(_schema, jsonMsg));
            envelope.setPayload(object);
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
