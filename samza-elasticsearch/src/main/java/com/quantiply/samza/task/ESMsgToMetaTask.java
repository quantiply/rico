package com.quantiply.samza.task;


import com.quantiply.rico.elasticsearch.IndexRequestKey;
import com.quantiply.rico.elasticsearch.VersionType;
import com.quantiply.samza.MetricAdaptor;
import com.quantiply.samza.elasticsearch.AvroKeyIndexRequestFactory;
import com.quantiply.samza.serde.AvroSerde;
import com.quantiply.samza.serde.AvroSerdeFactory;
import org.apache.avro.data.RecordBuilder;
import org.apache.samza.config.Config;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;

import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * Take messages with all the metadata in the message itself
 * and move the metadata into the message key
 *
 * Inbound msg: JSON with metadata as variables starting with _
 * Outbound msg: metadata in an Avro key and the document as the message
 *
 * Requires JSON serde for messages and Avro serde for keys
 *
 */
public class ESMsgToMetaTask extends BaseTask {
    private SystemStream outStream;

    @Override
    protected void _init(Config config, TaskContext context, MetricAdaptor metricAdaptor) throws Exception {
        registerDefaultHandler(this::processMsg);
        outStream = getSystemStream("out");
    }

    private void processMsg(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) throws Exception {
        Map<String, Object> document = (Map<String, Object>)envelope.getMessage();
        IndexRequestKey.Builder keyBuilder = IndexRequestKey.newBuilder().setId((String)document.get("_id"));
        if (document.containsKey("_version") && document.get("_version") instanceof Long) {
            keyBuilder.setVersion((Long)document.get("_version"));
            document.remove("_version");
        }
        if (document.containsKey("_version_type") && document.get("_version_type") instanceof String) {
            keyBuilder.setVersionType(VersionType.valueOf((String) document.get("_version_type")));
            document.remove("_version_type");
        }
        if (document.containsKey("_timestamp") && document.get("_timestamp") instanceof String) {
            keyBuilder.setTimestamp((String) document.get("_timestamp"));
            document.remove("_timestamp");
        }
        if (document.containsKey("_ttl") && document.get("_ttl") instanceof Long) {
            keyBuilder.setVersion((Long)document.get("_ttl"));
            document.remove("_ttl");
        }
        collector.send(new OutgoingMessageEnvelope(outStream, null, keyBuilder.build(), document));
    }
}
