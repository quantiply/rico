#ESPushTask

A reusable task for pulling data from Kafka and pushing to Elasticsearch.  It can read from one or more Kafka topics and map them to Elasticsearch indexes and doc types. The index names can be templatized so that data is partitioned by time. This task connects to the Elasticsearch cluster as a transport client and uses the bulk API.

## Suitability
* **Throughput** - Throughput depends on many factors including the index mapping but we've seen at least 10k documents/second per container (JVM) in production.
* **Delivery** - Samza guarantees at least once delivery by periodically checkpointing it's offsets in Kafka.  On recovery or restart, the job will continue from the previous checkpoint and may duplicate messages.  These can be detected and dealt with by including [metadata](#document-metadata).
* **Message Order** - You can guarantee message order by partitioning the Kafka topic in a meaningful way (say by user or account).  Messages in each partition are processed in order by Samza and the Elasticsearch bulk API guarantees sequentially processing. Alternatively, you can specify external version ids in the metadata that Elasticsearch can use to discard out-of-order writes.
* **Time-based partitioning** - You can define a pattern to use for the index name such that the job will periodically create new indexes (e.g. daily, weekly, quarterly, etc.).  If you include a timestamp in the [document metadata](#document-metadata), you will get deterministic, idempotent partitioning.
* **Error handling** - the Samza job will fail if it encounters an unexpected error while indexing.  When running Samza on YARN, it will automatically restart the job eight times by default.  If you want it to try to auto-recover indefinitely, you can set [yarn.container.retry.count](yarn-container-retry-count)=-1.

## Elasticsearch System Producer Status
The [Elasticsearch System Producer](https://issues.apache.org/jira/browse/SAMZA-654) with [metrics](https://issues.apache.org/jira/browse/SAMZA-733) is commited to the Apache Samza project but not released yet as of Sept 2015. It is included in this repository for now until Samza 0.10 is released. 

## Document Metadata

There are three ways to specify document metadata

### Kafka Key As Document Id

If you don't need additional metdata for the document beyond an id, you can set the document id (serialized as a UTF-8 string) as the key in the Kafka message.  This is preferred to embedding it in the message because the push task does not need to parse the message content.  It can forward the bytes directly to Elasticsearch for maximum throughput. 

### Kafka Key As Avro

If you need to specify additional metadata while leaving the document untouched (either for throughput or clean design), you can provide an Avro message as the Kafka key.

The fields in the Avro message (all of them optional) are:

* `id` - document id
* `version` - document [version](https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-index_.html#index-versioning)
* `version_type` - INTERNAL, EXTERNAL, EXTERNAL_GTE, FORCE
* `timestamp` - document [timestamp](https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-timestamp-field.html)
* `timestamp_unix_ms` - timestamp (milliseconds since epoch) to choose the correct index for the message.
* `ttl` - document [ttl](https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-index_.html#index-ttl)

### Embedded Metadata

Encoding metadata as Avro is the most effecient method but not the most convenient.  The third alternative is to embed metadata into the JSON message itself.  The job will remove these special fields before indexing the document in Elasticsearch.

* `@timestamp` - timestamp (milliseconds since epoch) to choose the correct index for the message.
* `_id` - document id
* `_version` - document [version](https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-index_.html#index-versioning)
* `_version_type` - internal, external, external_gte, force
* `_timestamp` - document [timestamp](https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-timestamp-field.html)
* `_ttl` - document [ttl](https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-index_.html#index-ttl)


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
	
### Example Samza Options
```
task.class=com.quantiply.samza.task.ESPushTask

systems.es.samza.factory=org.apache.samza.system.elasticsearch.ElasticsearchSystemFactory
systems.es.client.factory=org.apache.samza.system.elasticsearch.client.TransportClientFactory
#When rico.es.metadata.source=key_doc_id, use
#systems.es.index.request.factory=org.apache.samza.system.elasticsearch.indexrequest.DefaultIndexRequestFactory
#When rico.es.metadata.source is key_avro or embedded
systems.es.index.request.factory=com.quantiply.samza.elasticsearch.AvroKeyIndexRequestFactory
systems.es.client.transport.host=localhost
systems.es.client.transport.port=9300
systems.es.bulk.flush.interval.ms=100
```

For more details on the Elasticsearch System Producer, it's available [here](https://github.com/apache/samza/blob/master/docs/learn/documentation/versioned/jobs/configuration-table.html#L666-L712) until Samza 0.10 is released.

### Options

Option  | Values
------------- | -------------
`rico.es.index.prefix`| Prefix for the Elasticsearch index name; it gets lowercased and combined with the date format (Elasticsearch requires lower case index names).
`rico.es.stream.<stream_name>.index.prefix`  | Same as above but for an individual stream
`rico.es.index.date.format`| Appended to the index prefix. Must be a valid [DateTimeFormatter pattern](https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html).
`rico.es.stream.<stream_name>.index.date.format`  | Same as above but for an individual stream
`rico.es.index.date.zone`| Timezone to use for the index date format.  Must be a valid [ZoneId name](https://docs.oracle.com/javase/8/docs/api/java/time/ZoneId.html). Defaults to system timezone.
`rico.es.stream.<stream_name>.index.date.zone`  | Same as above but for an individual stream
`rico.es.metadata.source`| Method for specifying metadata.  Valid values are `key_doc_id`, `key_avro`, and `embedded`.  Defaults to `key_doc_id`.
`rico.es.stream.<stream_name>.metadata.source`  | Same as above but for an individual stream
`rico.es.version.type.default`  | If this parameter is set, the job will use the value as the version_type for all documents which do not have version_type set in the metadata.  Value values are `internal`, `external`, `external_gte`, and `force`.
`rico.es.stream.<stream_name>.version.type.default`  | Same as above but for an individual stream
`rico.es.doc.type`| Elasticsearch doc type
`rico.es.stream.<stream_name>.doc.type`  | Same as above but for an individual stream

## Operations
### Metrics
Elasticsearch System Producer metrics will be included in your Samza container metrics.  Here's an example.

```
"org.apache.samza.system.elasticsearch.ElasticsearchSystemProducerMetrics": {
            "es-docs-updated": 157,
            "es-bulk-send-success": 105179,
            "es-docs-inserted": 588822,
            "es-version-conflicts": 0
},
```

If you use [rico-metrics](https://github.com/Quantiply/rico-metrics) to send these to statsd, they will be sent as gauges with this form:

`<prefix>.samza.<job-name>.<job-id>.container.<container-name>.es.producer.<metric>` 

### Recovering from poison pills

If you get a bad record in an input Kafka topic and it causes a MappingException when you try to index it, you'll need to move the job past the bad record(s).  You can use the [Samza checkpoint tool](manipulating-checkpoints-manually) to manually move the checkpoint ahead.  Or, when the job is stopped, you can delete the checkpoint topic and make sure when you start it that it picks up from the latest offset by setting `systems.kafka.samza.offset.default=upcoming`.

##TODO
* Add support for deletes (requires changes to system producer)
* Add metric for tracking latency from message origin time (if provided in the metadata) to the time when index request is acknowledged.  This would provide a metric for complete end-to-end pipeline latency.  Also requires changes to the system producer in the Samza project.