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

import org.apache.samza.config.Config;
import org.apache.samza.config.ConfigException;
import org.apache.samza.config.MapConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Optional;

/**
 * Elasticsearch configuration class to read elasticsearch specific configuration from Samza.
 */
public class ElasticsearchConfig extends MapConfig {

  public enum AuthType { NONE, BASIC }

  private static final Logger LOGGER = LoggerFactory.getLogger(ElasticsearchConfig.class);

  public static final String CONFIG_KEY_FLUSH_MAX_ACTIONS = "flush.max.actions";
  public static final String CONFIG_KEY_FLUSH_INTERVALS_MS = "flush.interval.ms";

  public static final String CONFIG_KEY_HTTP_URL = "http.url";
  public static final String CONFIG_KEY_HTTP_AUTH_TYPE = "http.auth.type";
  public static final String CONFIG_KEY_HTTP_AUTH_BASIC_USER = "http.auth.basic.user";
  public static final String CONFIG_KEY_HTTP_AUTH_BASIC_PASSWORD = "http.auth.basic.password";

  public ElasticsearchConfig(String name, Config config) {
    super(config.subset("systems." + name + "."));

    logAllSettings(this);
  }

  public String getHTTPURL() {
    return get(CONFIG_KEY_HTTP_URL, "http://localhost:9200");
  }

  public AuthType getAuthType() {
    String authStr = get(CONFIG_KEY_HTTP_AUTH_TYPE, "none").toUpperCase();
    return AuthType.valueOf(authStr);
  }

  public String getBasicAuthUser() {
    return get(CONFIG_KEY_HTTP_AUTH_BASIC_USER);
  }

  public String getBasicAuthPassword() {
    return get(CONFIG_KEY_HTTP_AUTH_BASIC_PASSWORD);
  }

  public int getBulkFlushMaxActions() {
    return getInt(CONFIG_KEY_FLUSH_MAX_ACTIONS, 1000);
  }

  public Optional<Integer> getBulkFlushIntervalMS() {
    if (containsKey(CONFIG_KEY_FLUSH_INTERVALS_MS)) {
      int intervalMs = getInt(CONFIG_KEY_FLUSH_INTERVALS_MS);
      if (intervalMs <= 0) {
        throw new ConfigException(String.format("%s must be > 0", CONFIG_KEY_FLUSH_INTERVALS_MS));
      }
      return Optional.of(intervalMs);
    } else {
      return Optional.empty();
    }
  }

  private void logAllSettings(Config config) {
    StringBuilder b = new StringBuilder();
    b.append("Elasticsearch (HTTP) System settings: ");
    b.append("\n");
    for (Map.Entry<String, String> entry : config.entrySet()) {
      b.append('\t');
      b.append(entry.getKey());
      b.append(" = ");
      b.append(entry.getValue());
      b.append("\n");
    }
    LOGGER.info(b.toString());
  }
}
