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
package com.quantiply.samza;

import org.apache.samza.system.SystemFactory;
import org.apache.samza.system.kafka.KafkaSystemFactory;

public class ConfigConst {
    public static final String DEFAULT_SYSTEM_NAME = "kafka";
    public static final SystemFactory DEFAULT_SYSTEM_FACTORY = new KafkaSystemFactory();
    public static final String METRICS_GROUP_NAME = "com.quantiply.rico";
    public final static String STREAM_NAME_PREFIX = "rico.streams.";
    public final static String DROP_ON_ERROR = "rico.drop.on.error";
    public final static String DROP_MAX_RATIO = "rico.drop.max.ratio";
    public final static String ENABLE_DROPPED_MESSAGE_LOG = "rico.dropped-messages.log.enable";
    public final static String DROPPED_MESSAGE_STREAM_NAME = "rico.dropped-messages.log.stream";
}
