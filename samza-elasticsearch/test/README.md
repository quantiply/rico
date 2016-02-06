
## Build

```
#Download and untar elasticsearch

#One time
./bin/grid install all

#From parent dir
mvn package -Dmaven.test.skip=true && rm -rf test/deploy/samza && mkdir -p test/deploy/samza && tar -xvf ./target/rico-samza-elasticsearch-*-dist.tar.gz -C test/deploy/samza
```

## Deployment
	#Elasticsearch
	#From ES root dir
	./bin/elasticsearch
	#From here
	curl -XPUT localhost:9200/shakespeare -d@conf/elasticsearch/shakespeare_mapping.json
	curl -XDELETE localhost:9200/shakespeare
	curl -XDELETE localhost:9200/embedded
	curl -XDELETE localhost:9200/jsonkey

	#Kakfa/Samza
	rm -rf /tmp/kafka-logs/ && rm -rf /tmp/zookeeper/ && ./bin/grid start all
	./bin/create-topics.sh
	./bin/load-topics.sh

	#View shakespeare data
	./deploy/confluent/bin/kafka-console-consumer --topic shakespeare \
             --zookeeper localhost:2181 \
             --from-beginning

	./bin/start-job.sh shakespeare

	#View embedded data
	./deploy/confluent/bin/kafka-console-consumer --topic embedded \
             --zookeeper localhost:2181 \
             --from-beginning

	./bin/start-job.sh embedded

	#View json-key data
	./deploy/confluent/bin/kafka-console-consumer --topic jsonkey --property="print.key=true" \
             --zookeeper localhost:2181 \
             --from-beginning

	./bin/start-job.sh jsonkey

	#View metrics
	./deploy/confluent/bin/kafka-console-consumer --topic sys.samza_metrics \
             --zookeeper localhost:2181 \
             --from-beginning
