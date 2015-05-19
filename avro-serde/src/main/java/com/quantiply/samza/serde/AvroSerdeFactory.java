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

import kafka.utils.VerifiableProperties;
import org.apache.samza.config.Config;
import org.apache.samza.config.ConfigException;
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
