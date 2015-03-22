package com.quantiply.rico.serde;

import com.quantiply.rico.Configuration;
import com.quantiply.rico.Envelope;
import com.quantiply.rico.errors.ConfigException;
import com.quantiply.rico.errors.SerializationException;

/**
 * Created by rhoover on 3/21/15.
 */
public interface StringSerde<T> {

    public void init(Configuration cfg) throws ConfigException;

    public Envelope<T> fromString(String msg) throws SerializationException;

    public void writeTo(Envelope<T> envelope, java.io.StringWriter writer) throws SerializationException;
}
