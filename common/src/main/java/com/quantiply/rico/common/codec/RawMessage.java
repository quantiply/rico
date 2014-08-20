package com.quantiply.rico.common.codec;

import java.util.Map;

/**
 * Object to hold messages
 */
public final class RawMessage {

    private final Map<String, String> headers;
    private final byte[] body;

    public RawMessage(byte[] body, Map<String, String> headers) {
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
    public byte[] body() {
        return this.body;
    }

}
