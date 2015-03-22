package com.quantiply.rico.serde;

import com.quantiply.rico.Configuration;
import com.quantiply.rico.Envelope;

/**
 * Created by rhoover on 3/21/15.
 */
public interface StringSerde<T> {

    public void init(Configuration cfg);

    public Envelope<T> fromString(String msg) throws Exception;

    public void writeTo(Envelope<T> envelope, java.io.StringWriter writer) throws Exception;
}
