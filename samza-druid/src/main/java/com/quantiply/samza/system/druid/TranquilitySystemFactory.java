package com.quantiply.samza.system.druid;

import org.apache.samza.config.Config;
import org.apache.samza.config.ConfigException;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemConsumer;
import org.apache.samza.system.SystemFactory;
import org.apache.samza.system.SystemProducer;

import java.util.Optional;

public class TranquilitySystemFactory implements SystemFactory {

  @Override
  public SystemConsumer getConsumer(String name, Config config, MetricsRegistry metricsRegistry) {
    throw new UnsupportedOperationException();
  }

  @Override
  public SystemProducer getProducer(String name, Config config, MetricsRegistry metricsRegistry) {
    TranquilityConfig tranquilityConfig = new TranquilityConfig(name, config);
    return new TranquilitySystemProducer(name,
        getBulkLoaderFactory(tranquilityConfig),
        getExtractor(tranquilityConfig),
        new TranquilitySystemProducerMetrics(name, metricsRegistry));
  }

  @Override
  public SystemAdmin getAdmin(String name, Config config) {
    return TranquilitySystemAdmin.getInstance();
  }


  protected static HTTPTranquilityLoaderFactory getBulkLoaderFactory(TranquilityConfig config) {
    return new HTTPTranquilityLoaderFactory(config);
  }

  protected static Optional<EventTimeExtractor> getExtractor(TranquilityConfig config) {
    String factoryClass = config.getEventTimeExtractorFactory();
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
