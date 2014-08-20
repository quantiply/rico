package com.quantiply.rico.common.codec;

import java.util.HashMap;
import java.util.Map;

/**
 * Object to hold deserialized messages
 */
public class Message<T> {

    //public static final String HDR_CONTENT_TYPE = "Content-Type";
    //public static final String HDR_SCHEMA_ID = "X-Rico-Schema-Id";
    
    private final Map<String, String> headers;
    private final T body;

    public Message(T body) {
        this(body, new HashMap<String, String>());
    }
    
    public Message(T body, Map<String, String> headers) {
        if (headers == null) {
            headers = new HashMap<String, String>();
        }
        this.body = body;
        this.headers = headers;
    }

    /**
     * The message headers
     */
    public Map<String, String> headers() {
        return this.headers;
    }

    /**
     * The message body
     */
    public T body() {
        return this.body;
    }

}
