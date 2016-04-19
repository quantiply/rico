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
    HTTPTranquilityLoader.WriterConfig loaderConf = new HTTPTranquilityLoader.WriterConfig(
        systemName,
        config.getHTTPURL(),
        new HTTPTranquilityLoader.HTTPClientConfig(config.getConnectTimeoutMs(), config.getReadTimeoutMs()),
        config.getBulkFlushMaxActions(),
        config.getBulkFlushIntervalMS()
    );
    return new HTTPTranquilityLoader(config.getDatasource(), loaderConf, Optional.of(onFlush));
  }

}
