package com.quantiply.rico.common.codec;

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
    public Headers getHeaders() {
        return this.headers;
    }

    /**
     * The message body
     */
    public T getBody() {
        return this.body;
    }

}
