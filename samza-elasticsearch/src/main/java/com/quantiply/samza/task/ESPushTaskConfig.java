package com.quantiply.samza.task;

import com.quantiply.rico.elasticsearch.VersionType;
import com.quantiply.samza.elasticsearch.AvroKeyIndexRequestFactory;
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

    public enum MetadataSrc { KEY_DOC_ID, KEY_AVRO, EMBEDDED }

    public static class ESIndexSpec {
        public final String input;
        public final MetadataSrc metadataSrc;
        public final String indexNamePrefix;
        public final String indexNameDateFormat;
        public final ZoneId indexNameDateZone;
        public final String docType;
        public final Optional<VersionType> defaultVersionType;

        public ESIndexSpec(String input, MetadataSrc metadataSrc, String indexNamePrefix, String indexNameDateFormat, ZoneId indexNameDateZone, String docType, Optional<VersionType> defaultVersionType) {
            this.input = input;
            this.metadataSrc = metadataSrc;
            this.indexNamePrefix = indexNamePrefix;
            this.indexNameDateFormat = indexNameDateFormat;
            this.indexNameDateZone = indexNameDateZone;
            this.docType = docType;
            this.defaultVersionType = defaultVersionType;
        }
    }

    public final static String CFS_ES_SYSTEM_NAME = "es";
    public final static String CFG_ES_STREAMS = "rico.es.streams";
    public final static String CFG_ES_STREAM_INPUT = "rico.es.stream.%s.input";
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
        String stream = "default";
        MetadataSrc metadataSrc = getMetadataSrc(stream, config);
        String indexNamePrefix = getDefaultConfigParam(config, CFG_ES_DEFAULT_INDEX_PREFIX, null);
        String indexNameDateFormat = getDefaultConfigParam(config, CFG_ES_DEFAULT_INDEX_DATE_FORMAT, null);
        ZoneId indexNameDateZone = ZoneId.of(getDefaultConfigParam(config, CFG_ES_DEFAULT_INDEX_DATE_ZONE, ZoneId.systemDefault().toString()));
        String docType = getDefaultConfigParam(config, CFG_ES_DEFAULT_DOC_TYPE, null);
        String defaultVersionTypeStr = config.get(CFG_ES_DEFAULT_VERSION_TYPE_DEFAULT);
        Optional<VersionType> defaultVersionType = getVersionType(defaultVersionTypeStr);
        return new ESIndexSpec(
                stream,
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
                .map(stream -> getStreamConfig(stream, config))
                .collect(Collectors.toMap(spec -> spec.input, Function.identity()));
    }

    private static ESIndexSpec getStreamConfig(String stream, Config config) {
        String input = getStreamConfigParam(stream, config, CFG_ES_STREAM_INPUT, null);
        MetadataSrc metadataSrc = getMetadataSrc(stream, config);
        String indexNamePrefix = getStreamConfigParam(stream, config, CFG_ES_STREAM_INDEX_PREFIX, config.get(CFG_ES_DEFAULT_INDEX_PREFIX));
        String indexNameDateFormat = getStreamConfigParam(stream, config, CFG_ES_STREAM_INDEX_DATE_FORMAT, config.get(CFG_ES_DEFAULT_INDEX_DATE_FORMAT));
        String defaultDateZoneStr = config.get(CFG_ES_DEFAULT_INDEX_DATE_ZONE, ZoneId.systemDefault().toString());
        ZoneId indexNameDateZone = ZoneId.of(getStreamConfigParam(stream, config, CFG_ES_STREAM_INDEX_DATE_ZONE, defaultDateZoneStr));
        String docType = getStreamConfigParam(stream, config, CFG_ES_STREAM_DOC_TYPE, config.get(CFG_ES_DEFAULT_DOC_TYPE));
        String defaultVersionTypeStr = config.get(String.format(CFG_ES_STREAM_VERSION_TYPE_DEFAULT, stream), config.get(CFG_ES_DEFAULT_VERSION_TYPE_DEFAULT));
        Optional<VersionType> defaultVersionType = getVersionType(defaultVersionTypeStr);
        return new ESIndexSpec(
                input,
                metadataSrc,
                indexNamePrefix,
                indexNameDateFormat,
                indexNameDateZone,
                docType,
                defaultVersionType
        );
    }

    private static Optional<VersionType> getVersionType(String defaultVersionTypeStr) {
        Optional<VersionType> defaultVersionType = Optional.empty();
        if (defaultVersionTypeStr != null) {
            defaultVersionType = Optional.of(VersionType.valueOf(defaultVersionTypeStr.toUpperCase()));
        }
        return defaultVersionType;
    }

    private static MetadataSrc getMetadataSrc(String stream, Config config) {
        String metadataSrcStr = getStreamConfigParam(stream, config, CFG_ES_STREAM_DOC_METADATA_SRC, config.get(CFG_ES_DEFAULT_DOC_METADATA_SRC)).toLowerCase();
        if (!METADATA_SRC_OPTIONS.contains(metadataSrcStr)) {
            throw new ConfigException(String.format("Bad value for metadata src param %s stream: %s.  Options are: %s",
                    stream, metadataSrcStr,
                    String.join(",", METADATA_SRC_OPTIONS)));
        }
        MetadataSrc metadataSrc = MetadataSrc.valueOf(metadataSrcStr.toUpperCase());
        if (metadataSrc == MetadataSrc.KEY_AVRO || metadataSrc == MetadataSrc.EMBEDDED) {
            String indexReqFactoryParam = String.format("systems.%s.index.request.factory", CFS_ES_SYSTEM_NAME);
            String indexReqFactoryStr = config.get(indexReqFactoryParam);
            if (indexReqFactoryStr == null || !indexReqFactoryStr.equals(AvroKeyIndexRequestFactory.class.getCanonicalName())) {
                throw new ConfigException(String.format("For the ES %s metadata source, %s must be set to %s",
                        metadataSrcStr, indexReqFactoryParam, AvroKeyIndexRequestFactory.class.getCanonicalName()));
            }
        }
        return metadataSrc;
    }

    private static String getDefaultConfigParam(Config config, String param, String defaultVal) {
        String val = config.get(param, defaultVal);
        if (val == null) {
            throw new ConfigException("Missing ES config param: " + param);
        }
        return val;
    }

    private static String getStreamConfigParam(String stream, Config config, String paramFormat, String defaultVal) {
        String param = String.format(paramFormat, stream);
        String val = config.get(param, defaultVal);
        if (val == null) {
            throw new ConfigException("Missing ES config param: " + param);
        }
        return val;
    }
}
