package com.quantiply.rico.serde;

import com.quantiply.rico.Configuration;
import com.quantiply.rico.Envelope;
import com.quantiply.rico.errors.ConfigException;
import com.quantiply.rico.errors.SerializationException;

public interface StringSerde {

    public void init(Configuration cfg) throws ConfigException;

    public Envelope fromString(String msg) throws SerializationException;

    public void writeTo(Envelope envelope, java.io.StringWriter writer) throws SerializationException;
}
