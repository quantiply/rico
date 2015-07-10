package com.quantiply.samza.task;

import com.quantiply.samza.MetricAdaptor;
import org.apache.samza.config.Config;
import org.apache.samza.config.ConfigException;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;

import java.nio.charset.StandardCharsets;

/**
Samza task for pushing to Elasticsearch
 */
public class ESPushTask extends BaseTask {
    private final static String CFG_ES_INDEX_STREAM = "rico.es.index";
    private SystemStream esIndexStream;

    @Override
    protected void _init(Config config, TaskContext context, MetricAdaptor metricAdaptor) throws Exception {
        esIndexStream = getESStream();
        registerDefaultHandler(this::processMsg);
        if (getErrorHandler().dropOnError()) {
            logger.warn("Task is configured to drop messages on error");
        }
    }

    private SystemStream getESStream() {
        String esIndexName = config.get(CFG_ES_INDEX_STREAM);
        if (esIndexName == null) {
            throw new ConfigException("Missing config property Elasticearch index: " + CFG_ES_INDEX_STREAM);
        }
        return new SystemStream("es", esIndexName);
    }

    private void processMsg(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) throws Exception {
        //Message key is used for the document id
        String id = new String((byte [])envelope.getKey(), StandardCharsets.UTF_8);
        if (logger.isDebugEnabled()) {
            logger.debug(String.format("Sending document to Elasticsearch with id %s", id));
        }
        collector.send(new OutgoingMessageEnvelope(esIndexStream, null, id, envelope.getMessage()));
    }
}
