## What is rico?

Rico is an collection of common tasks and utils on top of Apache Samza.

## What can it do for you?

* It has a base class which you can extend that takes care of [a lot of boilerplate and common job concerns](https://github.com/quantiply/rico/blob/master/docs/base_task/purpose.md).
* Reusable tasks
	* [Elasticsearch loader](https://github.com/quantiply/rico/blob/master/docs/common_tasks/es-push.md)
	* [Avro to JSON converter](https://github.com/quantiply/rico/blob/master/docs/common_tasks/avro-to-json.md)
* Support for [Avro serialization](https://github.com/quantiply/rico/blob/master/avro/src/main/java/com/quantiply/samza/serde/AvroSerde.java) using Confluent Schema Registry.
* Support for Jython on Samza including a [project generator](https://github.com/quantiply/generator-rico).
* HTTP-based [Elasticsearch System Producer](https://github.com/quantiply/rico/blob/master/samza-elasticsearch/src/main/java/com/quantiply/samza/system/elasticsearch/ElasticsearchSystemProducer.java)
* [Dropwizard metrics integration](https://github.com/quantiply/samza-coda-metrics)

## How do you get it?

### Maven 3

#Building With Maven 3

Add the Maven Repository

	 <repositories>
      <repository>
        <id>quantiply</id>
        <name>Quantiply Bintray Repository</name>
        <url>http://dl.bintray.com/quantiply/quantiply</url>
      </repository>
    </repositories>

Add dependencies

	 <properties>
	   <rico.version>1.0.11</rico.version>
	 </properties>

    <dependency>
      <groupId>com.quantiply.rico</groupId>
      <artifactId>rico-core</artifactId>
      <version>${rico.version}</version>
    </dependency>
    <dependency>
      <groupId>com.quantiply.rico</groupId>
      <artifactId>rico-samza-elasticsearch</artifactId>
      <version>${rico.version}</version>
    </dependency>
