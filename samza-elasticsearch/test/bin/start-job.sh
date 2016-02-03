DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
ROOT_DIR=${DIR}/..

export SAMZA_CONTAINER_NAME=$1
export JAVA_OPTS="-Dlog4j.configuration=file://${ROOT_DIR}/../src/main/resources/log4j.properties"

(cd ${ROOT_DIR}/deploy/samza && exec ./bin/run-job.sh --config-factory=org.apache.samza.config.factories.PropertiesConfigFactory --config-path=file://${ROOT_DIR}/../src/main/config/$1.properties)
