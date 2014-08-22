package com.quantiply.rico.common.codec;

import com.quantiply.schema.Headers;

/**
 * Object to hold messages
 */
public final class RawMessage {

    private final Headers headers;
    private final byte[] body;

    public RawMessage(byte[] body, Headers headers) {
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
    public byte[] body() {
        return this.body;
    }

}
