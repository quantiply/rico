package com.quantiply.rico.api;

import java.util.List;

public interface Processor<E> {
    void init(Configuration configuration, Context context) throws Exception;

    List<Envelope<E>> process(List<Envelope<E>> events) throws Exception;

    List<Envelope<E>> window() throws Exception;

    void shutdown() throws Exception;

}
