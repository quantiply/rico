package com.quantiply.samza;

import com.quantiply.elasticsearch.HTTPBulkLoader;
import org.apache.samza.system.OutgoingMessageEnvelope;

public class MsgToAction {

  public static HTTPBulkLoader.ActionRequest convert(OutgoingMessageEnvelope envelope) {
    return null;
  }

}
