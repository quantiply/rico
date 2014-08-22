package com.quantiply.rico.common;

public interface Function<I, O> {

    public O call(I input) throws Exception; 
    
}
