package com.quantiply.rico.common.codec;

public class SchemaMessage<T, S> extends Message<T> {

    private final S schema;
    
    public SchemaMessage(S schema, T body, Headers headers) {
        super(body, headers);
        this.schema = schema;
    }
    
    /**
     * The schema for this message
     */
    public S getSchema() {
        return this.schema;
    }

}
