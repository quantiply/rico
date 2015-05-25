package com.quantiply.samza;

import org.apache.samza.system.SystemFactory;
import org.apache.samza.system.kafka.KafkaSystemFactory;

public class ConfigConst {
    public static final String DEFAULT_SYSTEM_NAME = "kafka";
    public static final SystemFactory DEFAULT_SYSTEM_FACTORY = new KafkaSystemFactory();
    public static final String METRICS_GROUP_NAME = "com.quantiply.rico";
    public final static String STREAM_NAME_PREFIX = "rico.streams.";
    public final static String DROP_ON_ERROR = "rico.drop.on.error";
    public final static String ENABLE_DROPPED_MESSAGE_LOG = "rico.dropped-messages.log.enable";
    public final static String DROPPED_MESSAGE_STREAM_NAME = "rico.dropped-messages.log.stream";
}
