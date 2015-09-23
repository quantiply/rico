package com.quantiply.samza.elasticsearch;

import com.quantiply.rico.elasticsearch.IndexRequestKey;
import org.apache.samza.SamzaException;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.elasticsearch.indexrequest.IndexRequestFactory;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.index.VersionType;
import org.apache.samza.system.elasticsearch.indexrequest.DefaultIndexRequestFactory;

import java.util.Map;

public class IndexRequestFromKey implements IndexRequestFactory {
    private IndexRequestFactory defaultFactory = new DefaultIndexRequestFactory();


    @Override
    /**
     * Defaults to DefaultIndexRequestFactory behavior so that this factory can
     * be used in all three metadata source modes
     */
    public IndexRequest getIndexRequest(OutgoingMessageEnvelope envelope) {
        if (envelope.getKey() instanceof IndexRequestKey) {
            return getIndexRequestFromKey(envelope);
        }
        return defaultFactory.getIndexRequest(envelope);
    }

    private IndexRequest getIndexRequestFromKey(OutgoingMessageEnvelope envelope) {
        String[] parts = envelope.getSystemStream().getStream().split("/");
        if (parts.length != 2) {
            throw new SamzaException("Elasticsearch stream name must match pattern {index}/{type}");
        }
        String index = parts[0];
        String type = parts[1];
        IndexRequest indexRequest = new IndexRequest(index, type);

        IndexRequestKey indexRequestKey = (IndexRequestKey) envelope.getKey();
        if (indexRequestKey != null) {
            if (indexRequestKey.getId() != null) {
                indexRequest.id(indexRequestKey.getId().toString());
            }
            if (indexRequestKey.getVersion() != null) {
                indexRequest.version(indexRequestKey.getVersion());
            }
            if (indexRequestKey.getVersionType() != null) {
                indexRequest.versionType(VersionType.fromString(indexRequestKey.getVersionType().toString().toLowerCase()));
            }
            if (indexRequestKey.getTimestamp() != null) {
                indexRequest.timestamp(indexRequestKey.getTimestamp().toString());
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
