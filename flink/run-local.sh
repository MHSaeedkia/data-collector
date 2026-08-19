#!/usr/bin/env bash
set -euo pipefail

# Runs ONE Flink job in this shell as a plain JVM process — no Flink cluster, no Flink image, no
# jar upload. Every job's main() calls StreamExecutionEnvironment.getExecutionEnvironment(), which
# outside a cluster hands back a LocalStreamEnvironment and spins up an in-process MiniCluster.
#
# Usage: ./run-local.sh <job>    e.g. ./run-local.sh job-rebaser
# Ctrl-C stops the job. Job discovery is shared with run-job.sh (see job-discovery.sh).
#
# Why the classpath dance: every Flink/Kafka/Avro dependency is `provided`, because the cluster
# image already carries them in /opt/flink/lib (see normalizer/Dockerfile). The shaded jar is
# therefore thin and `java -jar` on it dies with NoClassDefFoundError. `-Dmdep.includeScope=test`
# resolves compile + provided + test, which is exactly that missing set — plus flink-clients, whose
# LocalExecutorFactory is what env.execute() looks up to start the MiniCluster.
#
# Caveats worth knowing before you trust the output:
#   - Cancel the cluster's copy first (../scripts/cancel-flink-jobs.sh) or both write the same
#     downstream topics and you get duplicates.
#   - Sources start at OffsetsInitializer.latest(), so a job started after its upstream misses
#     whatever the upstream produced in the gap — the downstream-first rule from the root Makefile
#     still applies when you run several by hand.
#   - MiniCluster state is in-memory only: Ctrl-C loses the keyed state of the stateful jobs.

source "$(dirname "${BASH_SOURCE[0]}")/job-discovery.sh"

# Docker-internal hostnames are the in-cluster defaults; from the host the same services answer on
# published ports. Anything already exported wins, so a remote broker is just an env var away.
export KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"
export SCHEMA_REGISTRY_URL="${SCHEMA_REGISTRY_URL:-http://localhost:8082}"
export POSTGRES_URL="${POSTGRES_URL:-jdbc:postgresql://localhost:5432/markets}"

JOB="${1:-}"
read -r PROJECT MODULE <<< "$(resolve_job "$JOB")"

if [[ -z "${PROJECT:-}" ]]; then
    [[ -n "$JOB" ]] && echo "ERROR: unknown job '$JOB'" || echo "Usage: $0 <job>"
    echo "Available jobs:"
    list_jobs
    exit 1
fi

POM="$PROJECT/${MODULE:+$MODULE/}pom.xml"
TARGET="$PROJECT/${MODULE:+$MODULE/}target"
CP_FILE="$TARGET/local-classpath.txt"

# The shade <mainClass> is the manifest entry point on the cluster; here it is the class we launch.
MAIN=$(sed -n 's:.*<mainClass>\(.*\)</mainClass>.*:\1:p' "$POM" | head -1)

# Build classes and resolve the runtime classpath in one reactor pass. Multi-module needs -am so
# common/ is built alongside; the module runs last, so its classpath is the one left in CP_FILE.
echo "==> Building $JOB and resolving classpath..."
mvn -f "$PROJECT/pom.xml" ${MODULE:+-pl "$MODULE" -am} package -q -DskipTests \
    dependency:build-classpath -Dmdep.includeScope=test -Dmdep.outputFile="$CP_FILE"

CP="$(cat "$CP_FILE"):$TARGET/classes"

if ! grep -q 'flink-clients' "$CP_FILE"; then
    echo "ERROR: flink-clients is not on $JOB's classpath, so env.execute() will fail with"
    echo "       'No ExecutorFactory found to execute the application'."
    echo "       Add org.apache.flink:flink-clients (test scope) to $POM."
    exit 1
fi

echo "==> Running $MAIN on a local MiniCluster (Ctrl-C to stop)"
echo "    kafka=$KAFKA_BOOTSTRAP_SERVERS registry=$SCHEMA_REGISTRY_URL postgres=$POSTGRES_URL"
exec java -cp "$CP" "$MAIN"
