/*
 * Copyright 2016 Quantiply Corporation. All rights reserved.
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
package com.quantiply.samza.system.druid;

import org.apache.samza.config.Config;
import org.apache.samza.config.ConfigException;
import org.apache.samza.config.MapConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Optional;

public class TranquilityConfig extends MapConfig {
  private static final Logger LOGGER = LoggerFactory.getLogger(new Object() {}.getClass().getEnclosingClass());

  public static final String CONFIG_KEY_DATASOURCE = "datasource";
  public static final String CONFIG_KEY_FLUSH_MAX_ACTIONS = "flush.max.records";
  public static final String CONFIG_KEY_FLUSH_INTERVALS_MS = "flush.interval.ms";
  public static final String CONFIG_KEY_HTTP_URL = "http.url";
  public static final String CONFIG_KEY_HTTP_CONNECT_TIMEOUT_MS = "http.connect.timeout.ms";
  public static final String CONFIG_KEY_HTTP_READ_TIMEOUT_MS = "http.read.timeout.ms";
  public static final String CONFIG_KEY_EVENT_TIME_EXTRACTOR_FACTORY = "event.time.extractor.factory";

  private final String systemName;

  public TranquilityConfig(String name, Config config) {
    super(config.subset("systems." + name + "."));
    this.systemName = name;

    logAllSettings(this);
  }

  public String getDatasource() {
    String datasource = get(CONFIG_KEY_DATASOURCE);
    if (datasource == null) {
      throw new ConfigException(String.format("Must specify the Druid datasource with param: systems.%s.%s", systemName, CONFIG_KEY_DATASOURCE));
    }
    return datasource;
  }

  public String getHTTPURL() {
    String url = get(CONFIG_KEY_HTTP_URL, "http://localhost:8200/v1/post/");
    if (url.endsWith("/")) {
      return url;
    }
    return url + "/";
  }

  public String getEventTimeExtractorFactory() {
    return get(CONFIG_KEY_EVENT_TIME_EXTRACTOR_FACTORY);
  }

  public int getConnectTimeoutMs() {
    return getInt(CONFIG_KEY_HTTP_CONNECT_TIMEOUT_MS, 60000);
  }

  public int getReadTimeoutMs() {
    return getInt(CONFIG_KEY_HTTP_READ_TIMEOUT_MS, 60000);
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
    b.append("Tranquility (HTTP) System settings: ");
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
