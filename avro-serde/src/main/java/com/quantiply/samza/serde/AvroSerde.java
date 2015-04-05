package com.quantiply.samza.serde;

import org.apache.samza.serializers.Serde;
import io.confluent.kafka.serializers.KafkaAvroDecoder;
import io.confluent.kafka.serializers.KafkaAvroEncoder;
import kafka.utils.VerifiableProperties;

public class AvroSerde implements Serde<Object> {
    private final KafkaAvroEncoder encoder;
    private final KafkaAvroDecoder decoder;

    public AvroSerde(VerifiableProperties encoderProps, VerifiableProperties decoderProps) {
        encoder = new KafkaAvroEncoder(encoderProps);
        decoder = new KafkaAvroDecoder(decoderProps);
    }

    @Override
    public Object fromBytes(byte[] bytes) {
        return decoder.fromBytes(bytes);
    }

    @Override
    public byte[] toBytes(Object msg) {
        return encoder.toBytes(msg);
    }
}
