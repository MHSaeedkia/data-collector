# Usage: make run-local MODULE=job-pair-extractor
MODULE ?=

run-local:
	./run-job.sh $(MODULE)

run-remote:
	FLINK_API=http://192.168.150.104:7070 ./run-job.sh $(MODULE)

test:
	mvn test
