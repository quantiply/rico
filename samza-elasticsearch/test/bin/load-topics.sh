DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
ROOT_DIR=${DIR}/..

# ${ROOT_DIR}/deploy/confluent/bin/kafka-console-producer \
#               --broker-list localhost:9092 --topic shakespeare \
#               --compression-codec lz4 --new-producer < ${ROOT_DIR}/data/shakespeare.json

# ${ROOT_DIR}/deploy/confluent/bin/kafka-console-producer \
#               --broker-list localhost:9092 --topic embedded \
#               --compression-codec lz4 --new-producer < ${ROOT_DIR}/data/embedded.json

${ROOT_DIR}/deploy/confluent/bin/kafka-console-producer \
              --broker-list localhost:9092 --topic jsonkey --property parse.key=true \
              --compression-codec lz4 --new-producer < ${ROOT_DIR}/data/jsonkey.json
