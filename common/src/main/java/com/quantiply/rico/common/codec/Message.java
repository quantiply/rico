package com.quantiply.rico.common.codec;

import com.quantiply.schema.Headers;

/**
 * Object to hold deserialized messages
 */
public class Message<T> {

    private final Headers headers;
    private final T body;

    public Message(T body, Headers headers) {
        this.body = body;
        this.headers = headers;
    }

    /**
     * The message headers
     */
    public Headers headers() {
        return this.headers;
    }

    /**
     * The message body
     */
    public T body() {
        return this.body;
    }

}
