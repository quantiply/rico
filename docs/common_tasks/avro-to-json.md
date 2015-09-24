#AvroToJSONTask

A generic Samza task to convert Avro messages to JSON.  JSON is often required for downstream consumers like Elasticsearch and Druid.

#How it works

* Avro messages are deserialized into Avro compiler-generated classes
* The tasks uses Jackson to introspect the Java objects and convert them to JSON.
	* It uses only public getters
	* It converts names to lowercase with underscores

###Example schema:
```
{
  "type" : "record",
  "name" : "ExampleEvent",
  "namespace" : "com.fubar",
  "doc": "Example Avro record",
  "fields" : [
    {
      "name": "timestamp",
      "type": "long",
      "doc": "Unix time in ms when it happened"
    },
    {
      "name": "my_description",
      "type": "string",
      "doc": "What happened"
    },
    {
      "name": "id",
      "type": ["null", "string"],
      "default": null,
      "doc": "UUID to uniquely identify event"
    }
  ]
}
```

###Example generated Java

Leaving a lot out for clarity

```
package com.fubar;  

public class ExampleEvent extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {

  public ExampleEvent() {}

  public java.lang.Long getTimestamp() {
    return timestamp;
  }

  public java.lang.CharSequence getMyDescription() {
    return id;
  }

  public java.lang.CharSequence getId() {
    return id;
  }
}
```

###Example JSON
```
{
    "timestamp": 99999999,
    "my_description": "it was a dark and stormy night...",
    "id": null
}
```

## Requirements
This task expects

  - the input messages to be Avro serialized using Confluent Platform
  - the Avro message will be deserialized into Avro Specific Records
  - the input and output topics are configured to use ByteSerde
  - the output topic name to be specified with the "rico.streams.out" property
  - if you want lag metrics, the Avro message should have a "header" fields with "created" and "timestamp" child fields.

## Configuration

These are the essential configuration parameters

```
rico.schema.registry.url=http://localhost:8081
rico.streams.out=my.stuff.json

task.class=com.quantiply.samza.task.AvroToJSONTask
task.inputs=kafka.my.stuff.avro

#This task requires byte serde
serializers.registry.byte.class=org.apache.samza.serializers.ByteSerdeFactory
systems.kafka.samza.key.serde=byte
systems.kafka.samza.msg.serde=byte
```