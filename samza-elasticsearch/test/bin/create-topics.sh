DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

#Create metrics topic
${DIR}/../deploy/confluent/bin/kafka-topics --zookeeper localhost:2181 --topic sys.samza_metrics --create --partitions 1 --replication-factor 1

${DIR}/../deploy/confluent/bin/kafka-topics --zookeeper localhost:2181 --topic shakespeare --create --partitions 4 --replication-factor 1

# ${DIR}/../deploy/confluent/bin/kafka-topics --zookeeper localhost:2181 --topic avro.key.es.load --create --partitions 4 --replication-factor 1
#
# ${DIR}/../deploy/confluent/bin/kafka-topics --zookeeper localhost:2181 --topic embedded.es.load --create --partitions 4 --replication-factor 1
