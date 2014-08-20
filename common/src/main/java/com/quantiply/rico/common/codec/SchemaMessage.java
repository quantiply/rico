package com.quantiply.rico.common.codec;

import java.util.Map;

public class SchemaMessage<T, S> extends Message<T> {

    private final S schema;
    private final String schemaId;
    
    public SchemaMessage(S schema, String schemaId, T body, Map<String, String> headers) {
        super(body, headers);
        this.schema = schema;
        this.schemaId = schemaId;
    }
    
    /**
     * The schema for this message
     */
    public S schema() {
        return this.schema;
    }

    /**
     * The schema ID for the schema
     */
    public String schemaId() {
        return this.schemaId;
    }

}
