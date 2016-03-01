package com.quantiply.samza.system.elasticsearch;

import org.apache.samza.config.ConfigException;
import org.apache.samza.config.MapConfig;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.*;

public class ElasticsearchConfigTest {
  private ElasticsearchConfig EMPTY_CONFIG = new ElasticsearchConfig(
      "es",
      new MapConfig(Collections.<String, String>emptyMap()));

  private ElasticsearchConfig configForProperty(String key, String value) {
    Map<String, String> mapConfig = new HashMap<>();
    mapConfig.put(key, value);
    return new ElasticsearchConfig("es", new MapConfig(mapConfig));
  }

  @Test
  public void testHTTPURL() throws Exception {
    assertEquals("http://localhost:9200", EMPTY_CONFIG.getHTTPURL());
    ElasticsearchConfig config = configForProperty("systems.es.http.url", "http://example.org:9000");
    assertEquals("http://example.org:9000", config.getHTTPURL());
  }

  @Test
  public void testTimeoutParams() throws Exception {
    assertEquals(60000, EMPTY_CONFIG.getConnectTimeoutMs());
    assertEquals(60000, EMPTY_CONFIG.getReadTimeoutMs());

    ElasticsearchConfig connectConfig = configForProperty("systems.es.http.connect.timeout.ms", "1000");
    assertEquals(1000, connectConfig.getConnectTimeoutMs());

    ElasticsearchConfig inactivityConfig = configForProperty("systems.es.http.read.timeout.ms", "500");
    assertEquals(500, inactivityConfig.getReadTimeoutMs());
  }

  @Test
  public void testAuthParams() throws Exception {
    assertEquals(ElasticsearchConfig.AuthType.NONE, EMPTY_CONFIG.getAuthType());

    ElasticsearchConfig config = configForProperty("systems.es.http.auth.type", "basic");
    assertEquals(ElasticsearchConfig.AuthType.BASIC, config.getAuthType());

    ElasticsearchConfig userConfig = configForProperty("systems.es.http.auth.basic.user", "foo");
    assertEquals("foo", userConfig.getBasicAuthUser());

    ElasticsearchConfig passConfig = configForProperty("systems.es.http.auth.basic.password", "bar");
    assertEquals("bar", passConfig.getBasicAuthPassword());
  }

  @Test
  public void testFlushMaxActions() throws Exception {
    assertEquals(1000, EMPTY_CONFIG.getBulkFlushMaxActions());

    ElasticsearchConfig config = configForProperty("systems.es.flush.max.actions", "500");
    assertEquals(500, config.getBulkFlushMaxActions());
  }

  @Test
  public void testFlushMaxIntervalMs() throws Exception {
    assertFalse(EMPTY_CONFIG.getBulkFlushIntervalMS().isPresent());

    ElasticsearchConfig config = configForProperty("systems.es.flush.interval.ms", "500");
    assertEquals(500, config.getBulkFlushIntervalMS().get().intValue());

    assertThatThrownBy(configForProperty("systems.es.flush.interval.ms", "junk")::getBulkFlushIntervalMS).isInstanceOf(NumberFormatException.class);

    assertThatThrownBy(configForProperty("systems.es.flush.interval.ms", "-1")::getBulkFlushIntervalMS).isInstanceOf(ConfigException.class)
        .hasMessageContaining("must be > 0");

  }

}