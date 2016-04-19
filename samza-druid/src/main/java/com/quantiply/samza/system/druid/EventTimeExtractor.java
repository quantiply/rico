package com.quantiply.samza.system.druid;

import org.apache.samza.system.OutgoingMessageEnvelope;

/**
 * Interface that plugins need to implement to extract event time (ms since epoch) from messages
 */
public interface EventTimeExtractor {

  long getEventTsMs(OutgoingMessageEnvelope envelope);

}
