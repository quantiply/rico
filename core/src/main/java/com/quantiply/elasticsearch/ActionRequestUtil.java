package com.quantiply.elasticsearch;

import com.quantiply.rico.elasticsearch.Action;
import com.quantiply.rico.elasticsearch.ActionRequestKey;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;

public class ActionRequestUtil {

    public static ActionRequestKey getIndexRequestKeyFromCamus(IndexedRecord msg) {
        IndexedRecord header = null;
        Schema headerSchema = null;
        Schema.Field headerField = msg.getSchema().getField("header");
        if (headerField != null) {
            header = (IndexedRecord) msg.get(headerField.pos());
            headerSchema = headerField.schema();
        }
        return getIndexRequestFromCamusHeader(header, headerSchema);
    }

    public static ActionRequestKey getIndexRequestFromCamusHeader(IndexedRecord header, Schema headerSchema) {
        ActionRequestKey.Builder builder = ActionRequestKey.newBuilder();
        builder.setAction(Action.INDEX);
        if (header != null && headerSchema != null) {
            Schema.Field tsField = headerSchema.getField("time");
            Schema.Field idField = headerSchema.getField("id");
            if (tsField == null) {
                //For backward compatibility
                tsField = headerSchema.getField("timestamp");
            }
            if (tsField != null) {
                Object ts = header.get(tsField.pos());
                if (ts instanceof Long) {
                    builder.setEventTsUnixMs((Long) ts);
                    builder.setPartitionTsUnixMs((Long) ts);
                }
            }
            if (idField != null) {
                Object id = header.get(idField.pos());
                if (id instanceof CharSequence) {
                    builder.setId((CharSequence) id);
                }
            }
        }
        return builder.build();
    }
}
