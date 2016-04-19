package com.quantiply.samza.system.druid;

import com.quantiply.druid.HTTPTranquilityLoader;

import java.util.Optional;
import java.util.function.Consumer;

/**
 * Creates HTTPTranquilityLoader instances based on properties from the Samza job.
 */
public class HTTPTranquilityLoaderFactory {
  private final TranquilityConfig config;

  public HTTPTranquilityLoaderFactory(TranquilityConfig config) {
    this.config = config;
  }

  public HTTPTranquilityLoader getBulkLoader(String systemName, Consumer<HTTPTranquilityLoader.BulkReport> onFlush) {
    HTTPTranquilityLoader.Config loaderConf = new HTTPTranquilityLoader.Config(
        systemName,
        config.getHTTPURL(),
        config.getBulkFlushMaxActions(),
        config.getBulkFlushIntervalMS()
    );
    return new HTTPTranquilityLoader(config.getDatasource(), loaderConf, Optional.of(onFlush));
  }

}
