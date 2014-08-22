package com.quantiply.rico.common.codec;

import com.quantiply.schema.Headers;

public class SchemaMessage<T, S> extends Message<T> {

    private final S schema;
    
    public SchemaMessage(S schema, T body, Headers headers) {
        super(body, headers);
        this.schema = schema;
    }
    
    /**
     * The schema for this message
     */
    public S schema() {
        return this.schema;
    }

}
