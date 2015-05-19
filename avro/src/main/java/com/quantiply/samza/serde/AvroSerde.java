/*
 * Copyright 2014-2015 Quantiply Corporation. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
