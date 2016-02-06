#ESPushTask

A reusable task for pulling data from Kafka and pushing to Elasticsearch.  It can read from one or more Kafka topics and map them to Elasticsearch indexes and doc types. The index names can be templatized so that data is partitioned by time. This task uses the Elasticsearch HTTP bulk API.

## Suitability
* **Throughput** - Throughput depends on many factors including the index mapping but we've seen at least 30k documents/second per container (JVM) in production. You can deploy as few or a many containers as you need up to the max number of partitions in the input Kafka topics.
* **Delivery** - Samza guarantees at least once delivery by periodically checkpointing it's offsets in Kafka.  On recovery or restart, the job will continue from the previous checkpoint and may duplicate messages.  These can be detected and dealt with by including [metadata](#document-metadata).
* **Message Order** - You can guarantee message order by partitioning the Kafka topic in a meaningful way (say by user or account).  Messages in each partition are processed in order by Samza and the Elasticsearch bulk API guarantees sequential processing. You can also specify external version ids in the metadata that Elasticsearch can use to discard out-of-order writes.
* **Time-based partitioning** - You can define a pattern to use for the index name such that the job will periodically create new indexes (e.g. daily, weekly, quarterly, etc.).  If you include a timestamp in the [document metadata](#document-metadata), you will get deterministic, idempotent partitioning.
* **Error handling** - the Samza job will fail if it encounters an unexpected error while indexing.  When running Samza on YARN, it will automatically restart the job eight times by default.  If you want it to try to auto-recover indefinitely, you can set [yarn.container.retry.count](yarn-container-retry-count)=-1.

## Elasticsearch System Producer Status
There is an [Elasticsearch System Producer](https://samza.apache.org/learn/documentation/0.10/jobs/configuration-table.html#elasticsearch) that's part of the Apache Samza project as of version 0.10. It uses the native (transport) protocol and which is tied to specify versions of Elasticsearch.  This task uses it's own [HTTP-based system producer](https://github.com/quantiply/rico/blob/master/samza-elasticsearch/src/main/java/com/quantiply/samza/system/elasticsearch/ElasticsearchSystemProducer.java).

## Document Metadata

There are three ways to specify document metadata

### Kafka Key As Document Id

If you don't need additional metdata for the document beyond an id, you can set the document id (serialized as a UTF-8 string) as the key in the Kafka message.  This is preferred to embedding it in the message because the push task does not need to parse the message content.  It can forward the content directly to Elasticsearch for maximum throughput. If you do not specify a document id, a default id will be constructed based on the Kafka topic, partition, and offset.  This guarantees idempotent inserts within a give Elasticsearch index. However, if you partition indexes by time, you can still get duplicates across partitions unless you also provide a timestamp.

### Kafka Key As Avro or JSON

If you need to updates or deletes or specify additional metadata while leaving the document untouched (either for throughput or clean design), you can provide an Avro or JSON message as the Kafka key. If you use Avro, then you must deploy the [Confluent Schema Registry](http://docs.confluent.io/1.0/schema-registry/docs/index.html) to store the Avro schemas.

The fields in the Avro or JSON message (all of them optional) are:

* `action` - INDEX, UPDATE, DELETE.  Defaults to INDEX.
*  `id` - document id. If not set, it constructs a key based on Kafka topic, partition, and offset.
* `version` - document [version](https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-index_.html#index-versioning)
* `version_type` - EXTERNAL, FORCE
* `partition_ts_unix_ms` - timestamp (milliseconds since epoch) to choose the correct index for the message. Must be set for updates and deletes.  If not set for inserts, we use the import time (non-deterministic, non-idempotent) for indexes partitioned by time.
* `event_ts_unix_ms` - timestamp (milliseconds since epoch)used to compute latency metric from event origin time, if given.

### Embedded Metadata

Encoding metadata as Avro or JSON is the most effecient method but not alwasy the most convenient.  The third alternative is to embed metadata into the JSON message itself.  The job will remove these special fields before indexing the document in Elasticsearch.

* `@timestamp` - timestamp (milliseconds since epoch) to choose the correct index for the message.
* `_id` - document id. If not set, it constructs a key based on Kafka topic, partition, and offset.
* `_version` - document [version](https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-index_.html#index-versioning)
* `_version_type` - external, force

## Time-based Partitioning

If you provide a document id and timestamp as metadata, you are guaranteed that a single copy of the document with end up in the correct index.  However, if you do not provide a timestamp, the job will use the current wall clock time for partitioning and you may end up with documents in the "wrong" index as well as duplicates across partitions.  Documents may end up in the "wrong" index if they arrive late and get imported after the wall clock has moved on to a new index.  Documents may be written to two indexes if a batch index request is re-tried after a partial failure or after the Samza job is restarted.

## Configuration

We'll start with a few examples.  This job reads from a single topic, expects the document id as the key in the Kafka message, and creates monthly indexes based time of import in UTC.  An example index is `apache_logs.2015-09` with doctype `log`.

### Reading from a single topic with doc id as key

	rico.es.index.prefix=apache_logs
	rico.es.index.date.format=.yyyy-MM
	#Kibana index patterns use UTC time - https://www.elastic.co/guide/en/kibana/current/settings.html#settings-create-pattern
	rico.es.index.date.zone=Etc/UTC
	rico.es.doc.type=log

### Using JSON key for metadata
    rico.schema.registry.url=http://localhost:8081
    rico.es.index.date.format=.yyyy-MM
    rico.es.index.date.zone=Etc/UTC
    rico.es.doc.type=log
    rico.es.metadata.source=key_json

### Using Avro key for metadata
    rico.schema.registry.url=http://localhost:8081
    rico.es.index.date.format=.yyyy-MM
    rico.es.index.date.zone=Etc/UTC
    rico.es.doc.type=log
    rico.es.metadata.source=key_avro

### Reading from multiple topics

In this example, the job will pull from two topics and index them in daily indexes with metadata embedded in the each message.

	#Map Kafka topics to stream names: apache and tomcat
	rico.streams.apache=web.apache.log
	rico.streams.tomcat=app.tomcat.log
	
	#ES defaults
	rico.es.index.date.format=.yyyy-MM-dd
	rico.es.index.date.zone=Etc/UTC
	rico.es.doc.type=log
	rico.es.metadata.source=embedded
	
	#List all streams to index in ES
	rico.es.streams=apache,tomcat
	
	#Override defaults as needed for each stream
	rico.es.stream.apache.index.prefix=apache_logs
	rico.es.stream.apache.index.prefix=tomcat_logs
	
### Samza Options

This task expects the byte serde to be set for all keys and messages read from Kafka. See the example configuration below for the required value of `systems.es.index.request.factory`.

For more details on the Elasticsearch system producer config, it's available [here](https://github.com/apache/samza/blob/master/docs/learn/documentation/versioned/jobs/configuration-table.html#L666-L712) until Samza 0.10 is released.

```
task.class=com.quantiply.samza.task.ESPushTask

#This task requires byte serde
serializers.registry.byte.class=org.apache.samza.serializers.ByteSerdeFactory
systems.kafka.samza.key.serde=byte
systems.kafka.samza.msg.serde=byte

systems.es.samza.factory=com.quantiply.samza.system.elasticsearch.ElasticsearchSystemFactory
systems.es.http.host=localhost
systems.es.http.port=9200
systems.es.flush.interval.ms=500
systems.es.flush.max.actions=1000
```

### Configuration

#### Task Parameters

Option  | Values
------------- | -------------
`rico.schema.registry.url`|Url for the Confluent Schema Registry if using Avro
`rico.es.index.prefix`| Prefix for the Elasticsearch index name; it gets lowercased and combined with the date format (Elasticsearch requires lower case index names).
`rico.es.stream.<stream_name>.index.prefix`  | Same as above but for an individual stream
`rico.es.index.date.format`| Appended to the index prefix. Must be a valid [DateTimeFormatter pattern](https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html).
`rico.es.stream.<stream_name>.index.date.format`  | Same as above but for an individual stream
`rico.es.index.date.zone`| Timezone to use for the index date format.  Must be a valid [ZoneId name](https://docs.oracle.com/javase/8/docs/api/java/time/ZoneId.html). Defaults to system timezone.
`rico.es.stream.<stream_name>.index.date.zone`  | Same as above but for an individual stream
`rico.es.metadata.source`| Method for specifying metadata.  Valid values are `key_doc_id`, `key_json`, `key_avro`, and `embedded`.  Defaults to `key_doc_id`.
`rico.es.stream.<stream_name>.metadata.source`  | Same as above but for an individual stream
`rico.es.version.type.default`  | If this parameter is set, the job will use the value as the version_type for all documents which do not have version_type set in the metadata.  Value values are `external` and `force`.
`rico.es.stream.<stream_name>.version.type.default`  | Same as above but for an individual stream
`rico.es.doc.type`| Elasticsearch doc type
`rico.es.stream.<stream_name>.doc.type`  | Same as above but for an individual stream

#### System Parameters

Option  | Values
------------- | -------------
`system.<*>.samza.factory`|`com.quantiply.samza.system.elasticsearch.ElasticsearchSystemFactory`
`system.<*>.http.host`| Elasticsearch host name
`system.<*>.http.port`| Elasticsearch port for HTTP API
`system.<*>.http.port`| Elasticsearch port for HTTP API
`system.<*>.http.auth.type`| HTTP authentication type: `none` or `basic`.  Defaults to `none`
`system.<*>.http.auth.basic.user`| HTTP basic auth user
`system.<*>.http.auth.basic.password`| HTTP basic auth password

## Operations
### Metrics
Elasticsearch System Producer metrics will be included in your Samza container metrics.  Here's an example.

```
    "com.quantiply.samza.system.elasticsearch.ElasticsearchSystemProducerMetrics": {
      "es-bulk-send-wait-ms": {
        "75thPercentile": 35,
        "98thPercentile": 58,
        "min": 21,
        "median": 28,
        "95thPercentile": 49,
        "99thPercentile": 72,
        "max": 209,
        "mean": 31.48396552511989,
        "999thPercentile": 181,
        "type": "histogram",
        "stdDev": 12.848864565540332
      },
      "es-docs-updated": 0,
      "es-docs-created": 0,
      "es-lag-from-receive-ms": {
        "75thPercentile": 66,
        "98thPercentile": 106,
        "min": 40,
        "median": 54,
        "95thPercentile": 91,
        "99thPercentile": 142,
        "max": 1067,
        "mean": 61.19071362009049,
        "999thPercentile": 1067,
        "type": "histogram",
        "stdDev": 40.219517285244955
      },
      "es-docs-indexed": 1113960,
      "es-bulk-send-trigger-max-interval": 1,
      "es-lag-from-origin-ms": {
        "75thPercentile": 0,
        "98thPercentile": 0,
        "min": 0,
        "median": 0,
        "95thPercentile": 0,
        "99thPercentile": 0,
        "max": 0,
        "mean": 0,
        "999thPercentile": 0,
        "type": "histogram",
        "stdDev": 0
      },
      "es-docs-deleted": 0,
      "es-version-conflicts": 0,
      "es-bulk-send-trigger-max-actions": 1113,
      "es-bulk-send-trigger-flush-cmd": 0,
      "es-bulk-send-batch-size": {
        "75thPercentile": 1000,
        "98thPercentile": 1000,
        "min": 960,
        "median": 1000,
        "95thPercentile": 1000,
        "99thPercentile": 1000,
        "max": 1000,
        "mean": 999.9496016343502,
        "999thPercentile": 1000,
        "type": "histogram",
        "stdDev": 1.418941376819364
      },
      "es-bulk-send-success": 1114
    },
```

If you use [rico-metrics](https://github.com/Quantiply/rico-metrics) to send these to statsd, they will be sent as gauges with this form:

`<prefix>.samza.<job-name>.<job-id>.container.<container-name>.es.producer.<metric>` 

### Recovering from poison pills

If you get a bad record in an input Kafka topic and it causes a MappingException when you try to index it, you'll need to move the job past the bad record(s).  You can use the [Samza checkpoint tool](manipulating-checkpoints-manually) to manually move the checkpoint ahead.  Or, when the job is stopped, you can delete the checkpoint topic and make sure when you start it that it picks up from the latest offset by setting `systems.kafka.samza.offset.default=upcoming`.
