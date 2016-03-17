
cd test
bin/grid install all

bin/grid start imply

In a different window:
bin/grid start kafka

bin/generate-test-data.sh clicks1

bin/grid start yarn
bin/run-on-yarn.sh
