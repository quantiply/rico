package com.quantiply.rico.elasticsearch;

import com.quantiply.rico.Configuration;
import com.quantiply.rico.Context;
import com.quantiply.rico.Envelope;
import com.quantiply.rico.Processor;
import org.apache.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ElasticsearchProcessor implements Processor {

    private final static Logger LOG = Logger.getLogger(ElasticsearchProcessor.class);
    private TransportClient _client;
    private String _type;
    private String _index;
    private List<Envelope> _localCache = new ArrayList<>();
    private int BULK_BATCH_SIZE ;

    @Override
    public void init(Configuration cfg, Context context) throws Exception {

        String clusterName = cfg.getString("cluster");
        String host = cfg.getString("host");
        int port = cfg.getInt("port");
        _type = cfg.getString("type");
        _index = cfg.getString("index");
        BULK_BATCH_SIZE = cfg.getInt("bulk.batch.size");

        Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", clusterName).build();
        _client = new TransportClient(settings);
        _client.addTransportAddress(new InetSocketTransportAddress(host, port));

        // Fail if the cluster is not healthy.
        ClusterHealthResponse health = _client.admin().cluster().health(new ClusterHealthRequest()).actionGet();
        LOG.info("Health of cluster [ " + clusterName + " ] :" + health.getStatus());
        if (health.getStatus().value() != 0) {
            // throw new RuntimeException("Cluster is not healthy!");
        }
        LOG.info("Created ES client for cluster [ " + clusterName + " ] at " + host + ":" + port);


    }

    @Override
    public Envelope process(Envelope event) throws Exception {
        _localCache.add(event);

        if(_localCache.size() == BULK_BATCH_SIZE){
            sendEventsFromCache();
        }

        return null;
    }

    private void sendEventsFromCache(){
        BulkRequestBuilder bulkRequest = _client.prepareBulk();

        for(Envelope event: _localCache) {
            Map<String, Object> data = (Map<String, Object>) event.getBody();
            bulkRequest.add(_client.prepareIndex(_index, _type).setSource(data));
        }


        BulkResponse result = bulkRequest.execute().actionGet();
        LOG.debug("Processed " + _localCache.size() + "events.");
        if (result.hasFailures()) {
            throw new ElasticsearchException(result.buildFailureMessage());
        }
    }

    @Override
    public List window() throws Exception {
        sendEventsFromCache();
        return null;
    }

    @Override
    public void shutdown() throws Exception {
        _client.close();
    }

}
