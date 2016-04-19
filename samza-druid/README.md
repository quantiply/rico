
#From parent dir
mvn package -Dmaven.test.skip=true && rm -rf test/deploy/samza && mkdir -p test/deploy/samza && tar -xvf ./target/rico-samza-druid-*-dist.tar.gz -C test/deploy/samza

cd test
bin/grid install all

bin/grid start imply

In a different window:
bin/grid start kafka

bin/generate-test-data.sh clicks1

bin/grid start yarn
bin/run-on-yarn.sh
