package com.quantiply.samza.serde;

import org.apache.samza.config.Config;

import java.io.StringWriter;

public class StringStringSerde implements StringSerde {
    @Override
    public void init(Config cfg) throws Exception {}

    @Override
    public Object fromString(String msg) throws Exception {
        return msg;
    }

    @Override
    public void writeTo(Object msg, StringWriter writer) throws Exception {
        writer.write(msg.toString());
    }

}
