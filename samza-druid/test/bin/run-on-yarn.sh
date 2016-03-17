cd ..
mvn clean package
echo "y" | rm -rf test/deploy/samza/*
mkdir -p test/deploy/samza && tar -xvf target/hello-samza-0.0.1-dist.tar.gz -C test/deploy/samza
cd -
deploy/samza/bin/run-job.sh --config-factory=org.apache.samza.config.factories.PropertiesConfigFactory --config-path=file://$PWD/deploy/samza/config/tranquility-yarn.properties
