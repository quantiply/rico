package com.quantiply.rico;

import java.util.List;

public interface Processor {
    void init(Configuration configuration, Context context) throws Exception;

    List<Envelope> process(List<Envelope> events) throws Exception;

    List<Envelope> window() throws Exception;

    void shutdown() throws Exception;

}
