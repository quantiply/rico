package com.quantiply.rico.common.codec;

import java.io.IOException;

public class StringDecoder {
    
    private RawMessageDecoder decoder = new RawMessageDecoder();
    
    public StringMessage decode(byte[] payload) throws IOException {
        RawMessage raw = decoder.decode(payload);
        return new StringMessage(bodyFromBytes(raw), raw.getHeaders());
    }
    
    protected String bodyFromBytes(RawMessage raw) {
        return new String(raw.getBody(), java.nio.charset.StandardCharsets.UTF_8);
    }

}
