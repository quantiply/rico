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
package com.quantiply.samza.task;

import com.codahale.metrics.Meter;
import com.quantiply.samza.ConfigConst;
import com.quantiply.samza.admin.TaskInfo;
import org.apache.samza.config.Config;
import org.apache.samza.config.ConfigException;
import org.apache.samza.metrics.MetricsRegistryMap;
import org.apache.samza.serializers.JsonSerdeFactory;
import org.apache.samza.serializers.Serde;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemProducer;
import org.apache.samza.system.SystemStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class ErrorHandler {
    private final static String SYSTEM_PRODUCER_SOURCE = "rico-error-handler";
    private static Logger logger = LoggerFactory.getLogger(new Object() {}.getClass().getEnclosingClass());
    private final Config config;
    private final TaskInfo taskInfo;
    private Optional<SystemStream> droppedMsgStream;
    private Optional<SystemProducer> systemProducer;
    private boolean dropOnError;
    private Serde serde;
    private double dropMaxRatio = 1.0;

    public ErrorHandler(Config config, TaskInfo taskInfo) {
        this.config = config;
        this.taskInfo = taskInfo;
    }

    public void start() {
        serde = new JsonSerdeFactory().getSerde(SYSTEM_PRODUCER_SOURCE, config);
        dropOnError = config.getBoolean(ConfigConst.DROP_ON_ERROR, false);
        dropMaxRatio = config.getDouble(ConfigConst.DROP_MAX_RATIO, 0.5);
        if (dropMaxRatio <= 0.0 || dropMaxRatio >= 1.0) {
            throw new ConfigException(String.format("%s must be between 0.0 and 1.0", ConfigConst.DROP_MAX_RATIO));
        }
        boolean logDroppedMsgs = config.getBoolean(ConfigConst.ENABLE_DROPPED_MESSAGE_LOG, false);
        droppedMsgStream = Optional.ofNullable(config.get(ConfigConst.DROPPED_MESSAGE_STREAM_NAME))
                .map(streamName -> new SystemStream(ConfigConst.DEFAULT_SYSTEM_NAME, streamName));
        if (logDroppedMsgs && !droppedMsgStream.isPresent()) {
            throw new ConfigException(
                    String.format("No stream configured for dropped messages. Either set %s=false or %s",
                            ConfigConst.ENABLE_DROPPED_MESSAGE_LOG,
                            ConfigConst.DROPPED_MESSAGE_STREAM_NAME)
            );
        }
        systemProducer = droppedMsgStream.map(stream -> getSystemProducer(config));
        logDroppedMsgConfig();
    }

    public boolean dropOnError() {
        return dropOnError;
    }

    public double getDropMaxRatio() {
        return dropMaxRatio;
    }

    private void logDroppedMsgConfig() {
        StringBuilder builder = new StringBuilder();
        if (dropOnError) {
            builder.append("Dropping messages on error.");
            if (droppedMsgStream.isPresent()) {
                builder.append(" Sending metadata to stream: " + droppedMsgStream.get().getStream());
            }
            else {
                builder.append(" No drop stream configured");
            }
        }
        else {
            builder.append("Not dropping messages on error");
        }
        logger.info(builder.toString());
    }


    private SystemProducer getSystemProducer(Config config) {
        SystemProducer systemProducer = ConfigConst.DEFAULT_SYSTEM_FACTORY.getProducer(ConfigConst.DEFAULT_SYSTEM_NAME, config, new MetricsRegistryMap());
        systemProducer.register(SYSTEM_PRODUCER_SOURCE);
        systemProducer.start();
        return systemProducer;
    }

    /*
     * For handling unexpected errors.  Either kill the task for drop the messages
     * depending on configuration.
     */
    public void handleException(IncomingMessageEnvelope envelope, Exception e, BaseTask.StreamMetrics metrics) throws Exception {
        if (!dropOnError || hasTooManyErrors(metrics)) {
            throw e;
        }
        handleDroppedMessage(envelope, e, metrics.dropped);
    }

    public boolean hasTooManyErrors(BaseTask.StreamMetrics metrics) {
        long msgsDone = metrics.processed.getCount() + metrics.dropped.getCount();
        if (msgsDone > 500L) {
            double dropRatio = 1.0;
            if (metrics.dropped.getOneMinuteRate() > 0.0 && metrics.processed.getOneMinuteRate() > 0.0) {
                dropRatio = metrics.dropped.getOneMinuteRate()/metrics.processed.getOneMinuteRate();
            }
            if (dropRatio > dropMaxRatio) {
                logger.error(String.format("Error ratio (1min avg) %2f has exceeded threshold %f.", dropRatio, dropMaxRatio));
                return true;
            }
        }
        return false;
    }

    /*
     * For handling expected errors.  Drop the message
     */
    public void handleExpectedError(IncomingMessageEnvelope envelope, Exception e, BaseTask.StreamMetrics metrics) {
        handleDroppedMessage(envelope, e, metrics.dropped);
    }

    private void handleDroppedMessage(IncomingMessageEnvelope envelope, Exception e, Meter dropped) {
        droppedMsgStream.ifPresent(stream -> {
            if (logger.isDebugEnabled()) {
                logger.debug("Sending error info to dropped message stream: " + stream.getStream());
            }
            byte[] msg = serializeDroppedMessage(envelope, e);
            systemProducer.get().send(SYSTEM_PRODUCER_SOURCE, new OutgoingMessageEnvelope(stream, msg));
        });
        dropped.mark();
    }

    private byte[] serializeDroppedMessage(IncomingMessageEnvelope envelope, Exception e) {
        Map<String, Object> droppedMsg = new HashMap<>();
        droppedMsg.put("error_type", Optional.ofNullable(e.getCause()).orElse(e).getClass().getName());
        droppedMsg.put("error_message", Optional.ofNullable(e.getMessage()).orElse(""));
        droppedMsg.put("system", envelope.getSystemStreamPartition().getSystem());
        droppedMsg.put("stream", envelope.getSystemStreamPartition().getStream());
        droppedMsg.put("partition", envelope.getSystemStreamPartition().getPartition().getPartitionId());
        droppedMsg.put("offset", envelope.getOffset());
        droppedMsg.put("task", taskInfo.getTaskName());
        droppedMsg.put("container", taskInfo.getContainerName());
        droppedMsg.put("job_name", taskInfo.getJobName());
        droppedMsg.put("job_id", taskInfo.getJobId());

        return serde.toBytes(droppedMsg);
    }

    public void stop() {
        systemProducer.ifPresent(prdcr -> {
            prdcr.flush(SYSTEM_PRODUCER_SOURCE);
            prdcr.stop();
        });
    }
}
