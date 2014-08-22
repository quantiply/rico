package com.quantiply.rico.common.codec;

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
    public Headers getHeaders() {
        return this.headers;
    }

    /**
     * The message body
     */
    public byte[] getBody() {
        return this.body;
    }

}
