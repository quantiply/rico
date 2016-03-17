#!/bin/bash
if [[ $# -eq 0 ]] ; then
    echo 'bin/generate-test-data.sh <KAFKA_TOPIC>'
    exit 0
fi
echo "Generating 100 records every 30 secs and sending to topic $1 ...."
cd datagen
python clickstream.py  | kafkacat -b localhost -t $1 -P
cd -
