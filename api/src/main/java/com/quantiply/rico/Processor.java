package com.quantiply.rico;

import com.quantiply.rico.Configuration;
import com.quantiply.rico.Context;
import com.quantiply.rico.Envelope;

import java.util.List;

public interface Processor {
    void init(Configuration configuration, Context context) throws Exception;

    List<Envelope> process(List<Envelope> events) throws Exception;

    List<Envelope> window() throws Exception;

    void shutdown() throws Exception;

}
