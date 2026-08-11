# One entry point for every Flink project under flink/ — normalizer modules and merger alike.
# Usage: make run-local JOB=job-aggregator
#        make run-local JOB=merger
# Run `./run-job.sh` with no argument to list the available jobs.
JOB ?=

run-local:
	./run-job.sh $(JOB)

run-remote:
	FLINK_API=http://192.168.150.104:7070 ./run-job.sh $(JOB)

test:
	mvn -f normalizer/pom.xml test
	mvn -f merger/pom.xml test
