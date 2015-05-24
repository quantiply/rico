# Introduction

Many of these conventions arose writing our first Samza jobs and would need to be repeated over and over unless they were consolidated into the single place.

Metric and error handing functionality came out of wanting to answer these questions about a running job:

* How far behind is the job?
* How many errors have there been?
* Which messages caused the errors?

#Goals

## Decouple Job Logic From Topic Names

Samza jobs should not care about the names of the topics they interact with.  The base task provides a convention for referring to topics by "logical" names and mapping them to actual topic names via configuration.

In the following example, we define logical streams such as `svc-call-w-deploy` which get mapped to Kafka topics such as  `svc.call.w_deploy.c7tH4YaiTQyBEwAAhQzRXw`.

```
rico.streams.svc-call=svc.call.rcGuEPV1TpSgDQAAOipnrQ
rico.streams.deploy-svc=deploy.svc.tlrnsZOYQA6wrwAA4FLqZA
rico.streams.svc-call-w-deploy=svc.call.w_deploy.c7tH4YaiTQyBEwAAhQzRXw
```

In the task code, we refer to the streams by their logical names.

```java
    @Override
    protected void _init(Config config, TaskContext context, MetricAdaptor metricAdaptor) throws Exception {
        registerHandler("deploy-svc", this::processDeployMsg);
        registerHandler("svc-call", this::processCallMsg, EventStreamMetrics::new);
        outSysStream = getSystemStream("svc-call-w-deploy");
    }
```

This allows us to write jobs that can be used in different situations.  For example, if we need to re-partition a topic and give it a new name, we can easily re-configure our job to consume the new topic without changing any code.  

In the future, when we have standardized metadata about each topic (including it's message framing, encoding, and schema information), jobs should assert what they expect from a topic and fail immediate if the requirements are not met.

## Common Dispatch Logic

Most jobs with more than one input topic need to dispatch to different handlers for each input topic.  The base task allows you to register either

* a generic handler for any input message, or
* a handler per input topic

## Support for custom task-level and stream-level metrics

Some metrics apply to the whole task such as the rate of messages being produced. Other metrics apply to indivdual streams.  For example, we may want to know the mean lag from origin: the average amount of time between an event ocurring and the job seeing it.  This metric only makes sense per topic, not mixed across topics.  Same for the mean lag from previous stage.  The base task provides utilities for these common metrics and makes it easy to define custom task or stream-level metrics.

## Common Error Handling

If a job receives a message that it cannot handle, it can either fail or drop the message and continue.  When availability matters more than accuracy, you can configure the job to drop failed messages.  In such a case, how do you know how many messages are being dropped and which ones?

The base task allows you to configure if you want to drop messages.  When a message is dropped, it's tracked via a task metric and it's meta-data is written to a configurable topic so that the developer can later debug these messages and potentially re-process them.

## Avro Support

Until an official Avro serde exists for Confluent Platform, we provide one.

## Standard Partitioning Scheme for Avro Data

Getting partitioning right is perhaps the hardest part of creating Samza jobs as the jobs is left completely in the developers hands.  If you load data from tools like kafka-avro-console-producer (from Confluent Platform), you can get surprising results that don't line up with data partitioned via Samza job.

As of 0.9.0, Samza still uses the hashcode of the Java object to derive a partition id, unless you explicitly provide an partition id.  The new Kafka producer (used by kafka-avro-console-producer), however, uses a murmur2 hash of the message bytes to determine partition id.  Partitioning based on serialized bytes is preferred because it is well-defined for any message and does not require a Java object representation of the data.

However, when using Camus/Confluent Platform framing for key messages, the first part of the key contains a non-deterministic schema id.  The base task provides utility methods to hash based solely on the content of the key, not on it's schema id.

## Common Logging Context

The base task sets useful information in a [Mapped Diagnostic Context](http://logback.qos.ch/manual/mdc.html) so that logging messages are easily tied back to the original Kafka message.

