#AvroToJSONTask

A reusable task for pulling data from Kafka and pushing to Elasticsearch.  It can read from one or more Kafka topics and map them to Elasticsearch indexes and doc types. The indexes can be templatized so that data is partitioned by time. This task connects to the Elasticsearch cluster as a transport client and uses the bulk API.
