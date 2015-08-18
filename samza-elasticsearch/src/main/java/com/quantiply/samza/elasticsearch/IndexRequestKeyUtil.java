/*
 * Copyright 2014-2015 Quantiply Corporation. All rights reserved.
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
package com.quantiply.samza.elasticsearch;

import com.quantiply.rico.elasticsearch.IndexRequestKey;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;

public class IndexRequestKeyUtil {

    public static IndexRequestKey getIndexRequestKeyFromCamus(IndexedRecord msg) {
        IndexRequestKey.Builder builder = IndexRequestKey.newBuilder();
        Schema.Field headerField = msg.getSchema().getField("header");
        if (headerField != null) {
            IndexedRecord header = (IndexedRecord) msg.get(headerField.pos());
            Schema.Field tsField = headerField.schema().getField("timestamp");
            Schema.Field idField = headerField.schema().getField("id");

            if (tsField != null) {
                Object ts = header.get(tsField.pos());
                if (ts instanceof Long) {
                    builder.setTimestampUnixMs((Long)ts);
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
