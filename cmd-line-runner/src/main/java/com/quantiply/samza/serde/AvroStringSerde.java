/*
 * Copyright 2014-2015 Quantiply Corporation. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.quantiply.samza.serde;

import org.apache.avro.Schema;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonDecoder;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.apache.samza.config.Config;
import org.apache.samza.config.ConfigException;

import java.io.ByteArrayOutputStream;
import java.io.StringWriter;

public class AvroStringSerde implements StringSerde {
    public final static String CONFIG_INPUT_TYPE = "rico.string-serde.avro.input.type";
    private SpecificDatumReader<Object> _reader;
    private SpecificDatumWriter<Object> _writer;
    private JsonDecoder _decoder;

    @Override
    public void init(Config cfg) throws Exception {
        //get a reference to the Avro Specific Record class here
        String recordClassName = cfg.get(CONFIG_INPUT_TYPE);
        if (recordClassName == null) {
            throw new ConfigException("Avro input type not configured. Missing property: " + CONFIG_INPUT_TYPE);
        }
        Class clazz;
        try {
            clazz = Class.forName(recordClassName);
        }
        catch (ClassNotFoundException e) {
            throw new ConfigException("Avro record class not found: " + recordClassName);
        }
        try {
            SpecificRecord record = (SpecificRecord) clazz.newInstance();
            Schema _inSchema = record.getSchema();
            _reader = new SpecificDatumReader<>(_inSchema);
            _writer = new SpecificDatumWriter<>(_inSchema);
            _decoder = DecoderFactory.get().jsonDecoder(_inSchema, "");
        }
        catch (Exception e) {
            throw new ConfigException(e);
        }
    }

    @Override
    public Object fromString(String msg) throws Exception {
        _decoder.configure(msg);
        return _reader.read(null, _decoder);
    }

    @Override
    public void writeTo(Object msg, StringWriter writer) throws Exception {
        SpecificRecord record = (SpecificRecord) msg;
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        _writer.setSchema(record.getSchema());
        JsonEncoder encoder = EncoderFactory.get().jsonEncoder(record.getSchema(), out);
        _writer.write(msg, encoder);
        encoder.flush();
        writer.write(out.toString("UTF-8"));
    }
}

