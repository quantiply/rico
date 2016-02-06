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
  public void testHTTPHost() throws Exception {
    assertEquals("localhost", EMPTY_CONFIG.getHTTPHost());
    ElasticsearchConfig config = configForProperty("systems.es.http.host", "example.org");
    assertEquals("example.org", config.getHTTPHost());
  }

  @Test
  public void testHTTPPort() throws Exception {
    ElasticsearchConfig config = configForProperty("systems.es.http.port", "99200");
    assertEquals(99200, config.getHTTPPort());
    assertEquals(9200, EMPTY_CONFIG.getHTTPPort());
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