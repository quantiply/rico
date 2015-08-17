package com.quantiply.samza.task;

import com.quantiply.rico.elasticsearch.VersionType;
import com.quantiply.samza.elasticsearch.AvroKeyIndexRequestFactory;
import org.apache.samza.config.Config;
import org.apache.samza.config.ConfigException;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Need a map from topic to
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
        public final String indexNameDateZone;
        public final String docType;
        public final Optional<VersionType> defaultVersionType;

        public ESIndexSpec(String input, MetadataSrc metadataSrc, String indexNamePrefix, String indexNameDateFormat, String indexNameDateZone, String docType, Optional<VersionType> defaultVersionType) {
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
    public final static String CFG_ES_DEFAULT_DOC_METADATA_SRC = "rico.es.doc.metadata.source";
    public final static String CFG_ES_STREAM_DOC_METADATA_SRC = "rico.es.stream.%s.doc.metadata.source";
    public final static String CFG_ES_DEFAULT_INDEX_PREFIX = "rico.es.index.prefix";
    public final static String CFG_ES_STREAM_INDEX_PREFIX = "rico.es.stream.%s.index.prefix";
    public final static String CFG_ES_DEFAULT_INDEX_DATE_FORMAT = "rico.es.index.date.format";
    public final static String CFG_ES_STREAM_INDEX_DATE_FORMAT = "rico.es.stream.%s.index.date.format";

    public final static String CFG_ES_INDEX_DATE_ZONE = "rico.es.index.date.zone";
    public final static String CFG_ES_DOC_TYPE = "rico.es.doc.type";
    public final static String CFG_ES_VERSION_TYPE_DEFAULT = "rico.es.version.type.default";
    private final static HashSet<String> METADATA_SRC_OPTIONS = Arrays.stream(MetadataSrc.values()).map(v -> v.toString().toLowerCase()).collect(Collectors.toCollection(HashSet::new));

    public static Map<String,ESIndexSpec> getStreamMap(Config config) {
        //Get list of stream
        List<String> streams = config.getList(CFG_ES_STREAMS);
        return streams.stream()
                .map(stream -> getStreamConfig(stream, config))
                .collect(Collectors.toMap(spec -> spec.input, Function.identity()));
    }

    private static ESIndexSpec getStreamConfig(String stream, Config config) {
        String input = getStreamConfigParam(stream, config, CFG_ES_STREAM_INPUT, null);
        String metadataSrcStr = getStreamConfigParam(stream, config, CFG_ES_STREAM_DOC_METADATA_SRC, config.get(CFG_ES_DEFAULT_DOC_METADATA_SRC)).toLowerCase();
        if (!METADATA_SRC_OPTIONS.contains(metadataSrcStr)) {
            throw new ConfigException(String.format("Bad value for metadata src param on stream %s: %s.  Options are: %s",
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
        String indexNamePrefix = getStreamConfigParam(stream, config, CFG_ES_STREAM_INDEX_PREFIX, config.get(CFG_ES_DEFAULT_INDEX_PREFIX));
        String indexNameDateFormat = getStreamConfigParam(stream, config, CFG_ES_STREAM_INDEX_DATE_FORMAT, config.get(CFG_ES_DEFAULT_INDEX_DATE_FORMAT));
        return new ESIndexSpec(
                input,
                metadataSrc,
                indexNamePrefix,
                indexNameDateFormat,
                null,
                null,
                null
        );
    }

    private static String getStreamConfigParam(String stream, Config config, String paramFormat, String defaultVal) {
        String param = String.format(paramFormat, stream);
        String val = config.get(param, defaultVal);
        if (val == null) {
            throw new ConfigException("Missing ES config param: " + param);
        }
        return val;
    }


    /*

            indexNamePrefix = config.get(CFG_ES_INDEX_PREFIX);
        if (indexNamePrefix == null) {
            throw new ConfigException("Missing config property for Elasticsearch index prefix: " + CFG_ES_INDEX_PREFIX);
        }
        dateFormat = config.get(CFG_ES_INDEX_DATE_FORMAT, "");
        if (dateFormat == null) {
            throw new ConfigException("Missing config property Elasticsearch index date format: " + CFG_ES_INDEX_DATE_FORMAT);
        }
        dateZone = config.get(CFG_ES_INDEX_DATE_ZONE, ZoneId.systemDefault().toString());
        if (dateZone == null) {
            throw new ConfigException("Missing config property Elasticsearch index time zone: " + CFG_ES_INDEX_DATE_ZONE);
        }
        docType = config.get(CFG_ES_DOC_TYPE);
        if (docType == null) {
            throw new ConfigException("Missing config property for Elasticsearch index doc type: " + CFG_ES_DOC_TYPE);
        }
        String metadataSrcStr = config.get(CFG_ES_DOC_METADATA_SRC, "none").toLowerCase();
        if (!METADATA_SRC_OPTIONS.contains(metadataSrcStr)) {
            throw new ConfigException(String.format("Bad value for metadata src param: %s.  Options are: %s",
                    CFG_ES_DOC_METADATA_SRC,
                    String.join(",", METADATA_SRC_OPTIONS)));
        }
        metadataSrc = MetadataSrc.valueOf(metadataSrcStr.toUpperCase());
        if (metadataSrc == MetadataSrc.KEY || metadataSrc == MetadataSrc.EMBEDDED) {
            String indexReqFactoryParam = String.format("systems.%s.index.request.factory", CFS_ES_SYSTEM_NAME);
            String indexReqFactoryStr = config.get(indexReqFactoryParam);
            if (indexReqFactoryStr == null || !indexReqFactoryStr.equals(AvroKeyIndexRequestFactory.class.getCanonicalName())) {
                throw new ConfigException(String.format("For the ES %s metadata source, %s must be set to %s",
                        metadataSrcStr, indexReqFactoryParam, AvroKeyIndexRequestFactory.class.getCanonicalName()));
            }
        }
        String defaultVersionTypeStr = config.get(CFG_ES_VERSION_TYPE_DEFAULT);
        if (defaultVersionTypeStr != null) {
            defaultVersionType = Optional.of(VersionType.valueOf(defaultVersionTypeStr.toUpperCase()));
        }
     */

}