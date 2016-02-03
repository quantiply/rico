/*
 * Copyright 2014-2016 Quantiply Corporation. All rights reserved.
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
package com.quantiply.samza.task;

import com.quantiply.rico.elasticsearch.VersionType;
import org.apache.samza.config.Config;
import org.apache.samza.config.ConfigException;

import java.time.ZoneId;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Returns a map from topic to
 *   - metadata source for the stream
 *   - ES index name prefix
 *   - ES index name dateFormat
 *   - Timezone to use for dateFormat
 *   - ES doc type
 *   - default ES version type to use (optional)
 */
public class ESPushTaskConfig {

    public enum MetadataSrc { KEY_DOC_ID, KEY_AVRO, KEY_JSON, EMBEDDED }

    public static class ESIndexSpec {
        public final MetadataSrc metadataSrc;
        public final String indexNamePrefix;
        public final Optional<String> indexNameDateFormat;
        public final ZoneId indexNameDateZone;
        public final String docType;
        public final Optional<VersionType> defaultVersionType;

        public ESIndexSpec(MetadataSrc metadataSrc, String indexNamePrefix, Optional<String> indexNameDateFormat, ZoneId indexNameDateZone, String docType, Optional<VersionType> defaultVersionType) {
            this.metadataSrc = metadataSrc;
            this.indexNamePrefix = indexNamePrefix.toLowerCase(); //ES requires index names to be lower case
            this.indexNameDateFormat = indexNameDateFormat;
            this.indexNameDateZone = indexNameDateZone;
            this.docType = docType;
            this.defaultVersionType = defaultVersionType;
        }
    }

    public final static String CFS_ES_SYSTEM_NAME = "es";
    public final static String CFG_ES_STREAM_NAME = "bulk-http";
    public final static String CFG_ES_STREAMS = "rico.es.streams";
    public final static String CFG_ES_DEFAULT_DOC_METADATA_SRC = "rico.es.metadata.source";
    public final static String CFG_ES_STREAM_DOC_METADATA_SRC = "rico.es.stream.%s.metadata.source";
    public final static String CFG_ES_DEFAULT_INDEX_PREFIX = "rico.es.index.prefix";
    public final static String CFG_ES_STREAM_INDEX_PREFIX = "rico.es.stream.%s.index.prefix";
    public final static String CFG_ES_DEFAULT_INDEX_DATE_FORMAT = "rico.es.index.date.format";
    public final static String CFG_ES_STREAM_INDEX_DATE_FORMAT = "rico.es.stream.%s.index.date.format";
    public final static String CFG_ES_DEFAULT_INDEX_DATE_ZONE = "rico.es.index.date.zone";
    public final static String CFG_ES_STREAM_INDEX_DATE_ZONE = "rico.es.stream.%s.index.date.zone";
    public final static String CFG_ES_DEFAULT_DOC_TYPE = "rico.es.doc.type";
    public final static String CFG_ES_STREAM_DOC_TYPE = "rico.es.stream.%s.doc.type";
    public final static String CFG_ES_DEFAULT_VERSION_TYPE_DEFAULT = "rico.es.version.type.default";
    public final static String CFG_ES_STREAM_VERSION_TYPE_DEFAULT = "rico.es.stream.%s.version.type.default";

    private final static HashSet<String> METADATA_SRC_OPTIONS = Arrays.stream(MetadataSrc.values()).map(v -> v.toString().toLowerCase()).collect(Collectors.toCollection(HashSet::new));

    public static boolean isStreamConfig(Config config) {
        String streamList = config.get(CFG_ES_STREAMS);
        return streamList != null && streamList.length() > 0;
    }

    public static ESIndexSpec getDefaultConfig(Config config) {
        MetadataSrc metadataSrc = getMetadataSrc("default", getDefaultMetadataStrParam(config));
        String indexNamePrefix = getDefaultConfigParam(config, CFG_ES_DEFAULT_INDEX_PREFIX, null);
        Optional<String> indexNameDateFormat = Optional.ofNullable(config.get(CFG_ES_DEFAULT_INDEX_DATE_FORMAT));
        ZoneId indexNameDateZone = ZoneId.of(getDefaultDateZoneStr(config));
        String docType = getDefaultConfigParam(config, CFG_ES_DEFAULT_DOC_TYPE, null);
        String defaultVersionTypeStr = config.get(CFG_ES_DEFAULT_VERSION_TYPE_DEFAULT);
        Optional<VersionType> defaultVersionType = getVersionType(defaultVersionTypeStr);
        return new ESIndexSpec(
                metadataSrc,
                indexNamePrefix,
                indexNameDateFormat,
                indexNameDateZone,
                docType,
                defaultVersionType
        );
    }

    public static Map<String,ESIndexSpec> getStreamMap(Config config) {
        //Get list of stream
        List<String> streams = config.getList(CFG_ES_STREAMS);
        return streams.stream()
                .collect(Collectors.toMap(Function.identity(), stream -> getStreamConfig(stream, config)));
    }

    private static ESIndexSpec getStreamConfig(String stream, Config config) {
        String metadataSrcStr = getRequiredStreamConfigParam(stream, config, CFG_ES_STREAM_DOC_METADATA_SRC, getDefaultMetadataStrParam(config));
        MetadataSrc metadataSrc = getMetadataSrc(stream, metadataSrcStr);
        String indexNamePrefix = getRequiredStreamConfigParam(stream, config, CFG_ES_STREAM_INDEX_PREFIX, config.get(CFG_ES_DEFAULT_INDEX_PREFIX));
        Optional<String> indexNameDateFormat = Optional.ofNullable(getOptionalStreamConfigParam(stream, config, CFG_ES_STREAM_INDEX_DATE_FORMAT, config.get(CFG_ES_DEFAULT_INDEX_DATE_FORMAT)));
        ZoneId indexNameDateZone = ZoneId.of(getRequiredStreamConfigParam(stream, config, CFG_ES_STREAM_INDEX_DATE_ZONE, getDefaultDateZoneStr(config)));
        String docType = getRequiredStreamConfigParam(stream, config, CFG_ES_STREAM_DOC_TYPE, config.get(CFG_ES_DEFAULT_DOC_TYPE));
        String defaultVersionTypeStr = getOptionalStreamConfigParam(stream, config, CFG_ES_STREAM_VERSION_TYPE_DEFAULT, config.get(CFG_ES_DEFAULT_VERSION_TYPE_DEFAULT));
        Optional<VersionType> defaultVersionType = getVersionType(defaultVersionTypeStr);
        return new ESIndexSpec(
                metadataSrc,
                indexNamePrefix,
                indexNameDateFormat,
                indexNameDateZone,
                docType,
                defaultVersionType
        );
    }

    private static String getDefaultDateZoneStr(Config config) {
        return config.get(CFG_ES_DEFAULT_INDEX_DATE_ZONE, ZoneId.systemDefault().toString());
    }

    private static String getDefaultMetadataStrParam(Config config) {
        return config.get(CFG_ES_DEFAULT_DOC_METADATA_SRC, MetadataSrc.KEY_DOC_ID.name());
    }

    private static Optional<VersionType> getVersionType(String defaultVersionTypeStr) {
        Optional<VersionType> defaultVersionType = Optional.empty();
        if (defaultVersionTypeStr != null) {
            defaultVersionType = Optional.of(VersionType.valueOf(defaultVersionTypeStr.toUpperCase()));
        }
        return defaultVersionType;
    }

    private static MetadataSrc getMetadataSrc(String stream, String metadataSrcStrParam) {
        String metadataSrcStr = metadataSrcStrParam.toLowerCase();
        if (!METADATA_SRC_OPTIONS.contains(metadataSrcStr)) {
            throw new ConfigException(String.format("Bad value for metadata src param %s stream: %s.  Options are: %s",
                    stream, metadataSrcStr,
                    String.join(",", METADATA_SRC_OPTIONS)));
        }
        return MetadataSrc.valueOf(metadataSrcStr.toUpperCase());
    }

    private static String getDefaultConfigParam(Config config, String param, String defaultVal) {
        String val = config.get(param, defaultVal);
        if (val == null) {
            throw new ConfigException("Missing ES config param: " + param);
        }
        return val;
    }

    private static String getOptionalStreamConfigParam(String stream, Config config, String paramFormat, String defaultVal) {
        return getStreamConfigParam(stream, config, paramFormat, defaultVal, false);
    }

    private static String getRequiredStreamConfigParam(String stream, Config config, String paramFormat, String defaultVal) {
        return getStreamConfigParam(stream, config, paramFormat, defaultVal, true);
    }

    private static String getStreamConfigParam(String stream, Config config, String paramFormat, String defaultVal, boolean required) {
        String param = String.format(paramFormat, stream);
        String val = config.get(param, defaultVal);
        if (required && val == null) {
            throw new ConfigException("Missing ES config param: " + param);
        }
        return val;
    }
}
