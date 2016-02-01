package com.quantiply.samza.serde;

import org.apache.samza.config.Config;
import org.apache.samza.serializers.SerdeFactory;

public class JsonSerdeFactory implements SerdeFactory<Object> {
    @Override
    public JsonSerde getSerde(String s, Config config) {
        return new JsonSerde();
    }
}
