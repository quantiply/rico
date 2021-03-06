/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.quantiply.samza.system.elasticsearch;

import com.quantiply.elasticsearch.HTTPBulkLoader;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.system.*;

import java.util.function.Function;

/**
 * A {@link SystemFactory} for Elasticsearch.
 *
 * <p>This only supports the {@link SystemProducer} so all other methods return an
 * {@link UnsupportedOperationException}
 * <p>
 */
public class ElasticsearchSystemFactory implements SystemFactory {

  public final static Function<OutgoingMessageEnvelope,HTTPBulkLoader.ActionRequest> MSG_TO_ACTION = env -> (HTTPBulkLoader.ActionRequest)env.getMessage();

  @Override
  public SystemConsumer getConsumer(String name, Config config, MetricsRegistry metricsRegistry) {
    throw new UnsupportedOperationException();
  }

  @Override
  public SystemProducer getProducer(String name, Config config, MetricsRegistry metricsRegistry) {
    ElasticsearchConfig elasticsearchConfig = new ElasticsearchConfig(name, config);
    return new ElasticsearchSystemProducer(name,
                                           getBulkLoaderFactory(elasticsearchConfig),
                                           getClient(elasticsearchConfig),
                                           MSG_TO_ACTION,
                                           new ElasticsearchSystemProducerMetrics(name, metricsRegistry));
  }

  @Override
  public SystemAdmin getAdmin(String name, Config config) {
    return ElasticsearchSystemAdmin.getInstance();
  }


  protected static HTTPBulkLoaderFactory getBulkLoaderFactory(ElasticsearchConfig config) {
    return new HTTPBulkLoaderFactory(config);
  }

  protected static JestClient getClient(ElasticsearchConfig config) {
    JestClientFactory jestFactory = new JestClientFactory();
    HttpClientConfig.Builder httpClientBuilder = new HttpClientConfig.Builder(config.getHTTPURL());
    //Although we have a single writer thread per system producer, we set multiThreaded as true so that Jest
    //will used a pooled connection manager which re-establishes connections after they go stale
    httpClientBuilder.multiThreaded(true);
    httpClientBuilder.connTimeout(config.getConnectTimeoutMs());
    httpClientBuilder.readTimeout(config.getReadTimeoutMs());
    if (config.getAuthType().equals(ElasticsearchConfig.AuthType.BASIC)) {
      String user = config.getBasicAuthUser();
      String password = config.getBasicAuthPassword();
      if (user == null || password == null) {
        throw new SamzaException("Please specify a user and password for HTTP basic auth");
      }
      httpClientBuilder.defaultCredentials(user, password);
    }
    jestFactory.setHttpClientConfig(httpClientBuilder.build());
    return jestFactory.getObject();
  }

}
