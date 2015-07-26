package com.quantiply.samza.elasticsearch;

import com.quantiply.rico.elasticsearch.IndexRequestKey;
import org.apache.samza.SamzaException;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.elasticsearch.indexrequest.IndexRequestFactory;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.index.VersionType;

import java.util.Map;

public class AvroKeyIndexRequestFactory implements IndexRequestFactory {

    @Override
    public IndexRequest getIndexRequest(OutgoingMessageEnvelope envelope) {
        String[] parts = envelope.getSystemStream().getStream().split("/");
        if (parts.length != 2) {
            throw new SamzaException("Elasticsearch stream name must match pattern {index}/{type}");
        }
        String index = parts[0];
        String type = parts[1];
        IndexRequest indexRequest = new IndexRequest(index, type);

        IndexRequestKey indexRequestKey = (IndexRequestKey) envelope.getKey();
        if (indexRequestKey != null) {
            indexRequest.id(indexRequestKey.getId());
            if (indexRequestKey.getVersion() != null) {
                indexRequest.version(indexRequestKey.getVersion());
            }
            if (indexRequestKey.getVersionType() != null) {
                indexRequest.versionType(VersionType.fromString(indexRequestKey.getVersionType().toString().toLowerCase()));
            }
            if (indexRequestKey.getTimestamp() != null) {
                indexRequest.timestamp(indexRequestKey.getTimestamp());
            }
            if (indexRequestKey.getTtl() != null) {
                indexRequest.ttl(indexRequestKey.getTtl());
            }
        }

        Object partitionKey = envelope.getPartitionKey();
        if (partitionKey != null) {
            indexRequest.routing(partitionKey.toString());
        }

        Object message = envelope.getMessage();
        if (message instanceof byte[]) {
            indexRequest.source((byte[]) message);
        } else if (message instanceof Map) {
            indexRequest.source((Map) message);
        } else {
            throw new SamzaException("Unsupported message type: " + message.getClass().getCanonicalName());
        }

        return indexRequest;
    }

}
