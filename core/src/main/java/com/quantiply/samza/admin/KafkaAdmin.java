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
package com.quantiply.samza.admin;

import org.apache.samza.config.Config;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.SystemStreamMetadata;
import org.apache.samza.system.kafka.KafkaSystemFactory;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Helper for fetching Kafka metadata
 */
public class KafkaAdmin {

    public static int getNumPartitionsForStream(Config cfg, SystemStream systemStream) {
        Set<String> streamNames = new HashSet<>();
        streamNames.add(systemStream.getStream());
        SystemAdmin kafkaSystemAdmin = new KafkaSystemFactory().getAdmin(systemStream.getSystem(), cfg);
        Map<String, SystemStreamMetadata> metadata =
                kafkaSystemAdmin.getSystemStreamMetadata(streamNames);
        SystemStreamMetadata topicMetadata = metadata.get(systemStream.getStream());
        return topicMetadata.getSystemStreamPartitionMetadata().size();
    }
}
