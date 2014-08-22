package com.quantiply.rico.common.codec;

import java.io.IOException;

public abstract class BaseEncoder<T, M extends Message<T>> implements Encoder<T, M>{
    
    protected RawMessageEncoder encoder = new RawMessageEncoder();
    
    abstract protected byte[] getBodyBytes(M body) throws IOException;
    
    public byte[] encode(final M msg) throws IOException {
        return encoder.encode(getBodyBytes(msg), msg.getHeaders());
    }

}
