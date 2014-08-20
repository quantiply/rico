package com.quantiply.rico.common.codec;

import com.google.common.collect.ImmutableMap;

/**
 * Object to hold messages
 */
public final class Message {

    private final ImmutableMap<String, String> headers;
    private final byte[] body;

    public Message(byte[] body, ImmutableMap<String, String> headers) {
        this.body = body;
        this.headers = headers;
    }

    /**
     * The message headers
     */
    public ImmutableMap<String, String> headers() {
        return this.headers;
    }

    /**
     * The message body
     */
    public byte[] body() {
        return this.body;
    }

}
