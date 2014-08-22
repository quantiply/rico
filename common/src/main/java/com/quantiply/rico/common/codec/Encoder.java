package com.quantiply.rico.common.codec;

import java.io.IOException;

public interface Encoder<T, M extends Message<T>> {
    public byte[] encode(final M msg) throws IOException;
}
