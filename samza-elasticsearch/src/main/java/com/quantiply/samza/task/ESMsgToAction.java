package com.quantiply.samza.task;

import com.quantiply.elasticsearch.HTTPBulkLoader;
import org.apache.samza.system.OutgoingMessageEnvelope;

public class ESMsgToAction {

  public static HTTPBulkLoader.ActionRequest convert(OutgoingMessageEnvelope envelope) {
    //TODO - Implement this
    return null;
  }

}
