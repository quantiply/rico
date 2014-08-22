package com.quantiply.rico.common.codec;

public class StringMessage extends Message<String> {

    public StringMessage(String body, Headers headers) {
        super(body, headers);
    }

}
