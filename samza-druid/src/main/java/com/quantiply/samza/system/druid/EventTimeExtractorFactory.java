package com.quantiply.samza.system.druid;

import org.apache.samza.config.Config;

/**
 * Interface for factory to configure event time extractor plugins
 */
public interface EventTimeExtractorFactory {

  EventTimeExtractor getExtractor(Config config);

}
