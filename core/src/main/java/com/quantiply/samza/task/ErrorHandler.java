package com.quantiply.samza.task;

import com.quantiply.samza.ConfigConst;
import org.apache.samza.config.Config;
import org.apache.samza.config.ConfigException;
import org.apache.samza.metrics.MetricsRegistryMap;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemProducer;
import org.apache.samza.system.SystemStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

public class ErrorHandler {
    private final static String SYSTEM_PRODUCER_SOURCE = "rico-error-handler";
    private static Logger logger = LoggerFactory.getLogger(new Object() {}.getClass().getEnclosingClass());
    private final Config config;
    private Optional<SystemStream> droppedMsgStream;
    private Optional<SystemProducer> systemProducer;
    private boolean dropOnError;

    public ErrorHandler(Config config) {
        this.config = config;
    }

    public void start() {
        dropOnError = config.getBoolean(ConfigConst.DROP_ON_ERROR, false);
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

    private void logDroppedMsgConfig() {
        StringBuilder builder = new StringBuilder();
        if (dropOnError) {
            builder.append("Dropping messages on error.");
            if (droppedMsgStream.isPresent()) {
                builder.append(" Sending to stream: " + droppedMsgStream.get().getStream());
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

    public void handleError(IncomingMessageEnvelope envelope, Exception e) throws Exception {
        if (!dropOnError) {
            throw e;
        }
        droppedMsgStream.ifPresent(stream -> {
            if (logger.isDebugEnabled()) {
                logger.debug("Sending message to dropped message stream: " + stream.getStream());
            }
            //TODO - serialize the important stuff here to JSON
            byte[] msg = "Testing 1 2 3".getBytes();
            systemProducer.get().send(SYSTEM_PRODUCER_SOURCE, new OutgoingMessageEnvelope(stream, msg));
        });
    }

    public void stop() {
        systemProducer.ifPresent(prdcr -> {
            prdcr.flush(SYSTEM_PRODUCER_SOURCE);
            prdcr.stop();
        });
    }
}
