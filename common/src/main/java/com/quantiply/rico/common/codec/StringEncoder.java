package com.quantiply.rico.common.codec;

/**
 * Encodes Strings messages
 * 
 * @author rhoover
 *
 */
public class StringEncoder extends BaseEncoder<String, StringMessage> {

    @Override
    protected byte[] getBodyBytes(StringMessage msg) {
        return msg.getBody().getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }
}

