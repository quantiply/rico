package com.quantiply.samza.serde;

import com.quantiply.rico.errors.ConfigException;
import kafka.utils.VerifiableProperties;
import org.apache.samza.config.Config;
import org.apache.samza.serializers.SerdeFactory;

import java.util.Properties;

public class AvroSerdeFactory implements SerdeFactory<Object> {
    public static String CFG_SCHEMA_REGISTRY_URL = "confluent.schema.registry.url";

    @Override
    public AvroSerde getSerde(String s, Config config) {
        final String registryUrl = config.get(CFG_SCHEMA_REGISTRY_URL);
        if (registryUrl == null) {
            throw new ConfigException("Missing property: " + CFG_SCHEMA_REGISTRY_URL);
        }
        final String specificReader = config.get("confluent.specific.avro.reader", "true");
        final Properties encoderProps = new Properties();
        encoderProps.setProperty("schema.registry.url", registryUrl);
        final Properties decoderProps = new Properties();
        decoderProps.setProperty("schema.registry.url", registryUrl);
        decoderProps.setProperty("specific.avro.reader", specificReader);
        return new AvroSerde(new VerifiableProperties(encoderProps), new VerifiableProperties(decoderProps));
    }
}
