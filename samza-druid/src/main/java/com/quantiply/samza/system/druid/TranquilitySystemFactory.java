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
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemConsumer;
import org.apache.samza.system.SystemFactory;
import org.apache.samza.system.SystemProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

public class TranquilitySystemFactory implements SystemFactory {
  private Logger logger = LoggerFactory.getLogger(new Object() {}.getClass().getEnclosingClass());

  @Override
  public SystemConsumer getConsumer(String name, Config config, MetricsRegistry metricsRegistry) {
    throw new UnsupportedOperationException();
  }

  @Override
  public SystemProducer getProducer(String name, Config config, MetricsRegistry metricsRegistry) {
    TranquilityConfig tranquilityConfig = new TranquilityConfig(name, config);
    try {
      return new TranquilitySystemProducer(name,
          getBulkLoaderFactory(tranquilityConfig),
          getExtractor(tranquilityConfig, config),
          new TranquilitySystemProducerMetrics(name, metricsRegistry));
    }
    catch (Exception e) {
      logger.error("Could not create tranquility system producer", e);
      throw e;
    }
  }

  @Override
  public SystemAdmin getAdmin(String name, Config config) {
    return TranquilitySystemAdmin.getInstance();
  }


  protected static HTTPTranquilityLoaderFactory getBulkLoaderFactory(TranquilityConfig config) {
    return new HTTPTranquilityLoaderFactory(config);
  }

  protected static Optional<EventTimeExtractor> getExtractor(TranquilityConfig tranquilityConfig, Config config) {
    String factoryClass = tranquilityConfig.getEventTimeExtractorFactory();
    if (factoryClass == null) {
      return Optional.empty();
    }
    try {
      Class<EventTimeExtractorFactory> klass = (Class<EventTimeExtractorFactory>)Class.forName(factoryClass.trim());
      return Optional.of(klass.newInstance().getExtractor(config));
    } catch (ClassNotFoundException e) {
      throw new ConfigException("Event time extractor factory class not found: " + factoryClass);
    } catch (InstantiationException e) {
      throw new ConfigException("Could not instantiate event time extractor factory class: " + factoryClass);
    } catch (IllegalAccessException e) {
      throw new ConfigException("Could not access event time extractor factory class: " + factoryClass);
    }
  }
}
