package com.quantiply.elasticsearch;

import com.quantiply.rico.elasticsearch.Action;
import com.quantiply.rico.elasticsearch.ActionRequestKey;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;

public class ActionRequestUtil {

    public static ActionRequestKey getIndexRequestKeyFromCamus(IndexedRecord msg) {
        ActionRequestKey.Builder builder = ActionRequestKey.newBuilder();
        builder.setAction(Action.INDEX);
        Schema.Field headerField = msg.getSchema().getField("header");
        if (headerField != null) {
            IndexedRecord header = (IndexedRecord) msg.get(headerField.pos());
            Schema.Field tsField = headerField.schema().getField("timestamp");
            Schema.Field idField = headerField.schema().getField("id");

            if (tsField != null) {
                Object ts = header.get(tsField.pos());
                if (ts instanceof Long) {
                    builder.setEventTsUnixMs((Long)ts);
                    builder.setPartitionTsUnixMs((Long)ts);
                }
            }
            if (idField != null) {
                Object id = header.get(idField.pos());
                if (id instanceof CharSequence) {
                    builder.setId((CharSequence)id);
                }
            }
        }
        return builder.build();
    }
}
