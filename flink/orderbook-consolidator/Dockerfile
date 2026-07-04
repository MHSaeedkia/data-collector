FROM flink:2.2.0-scala_2.12-java21

# Kafka connector (source/sink)
RUN wget -q -P /opt/flink/lib/ \
    https://repo1.maven.org/maven2/org/apache/flink/flink-connector-kafka/5.0.0-2.2/flink-connector-kafka-5.0.0-2.2.jar

# Avro format + Confluent Schema Registry support
RUN wget -q -P /opt/flink/lib/ \
    https://repo1.maven.org/maven2/org/apache/flink/flink-avro/2.2.0/flink-avro-2.2.0.jar \
 && wget -q -P /opt/flink/lib/ \
    https://repo1.maven.org/maven2/org/apache/flink/flink-avro-confluent-registry/2.2.0/flink-avro-confluent-registry-2.2.0.jar \
 && wget -q -P /opt/flink/lib/ \
    https://repo1.maven.org/maven2/org/apache/avro/avro/1.11.4/avro-1.11.4.jar

# Jackson — required by avro at runtime (Flink shades its own copy, doesn't expose it)
RUN wget -q -P /opt/flink/lib/ \
    https://repo1.maven.org/maven2/com/fasterxml/jackson/core/jackson-databind/2.14.3/jackson-databind-2.14.3.jar \
 && wget -q -P /opt/flink/lib/ \
    https://repo1.maven.org/maven2/com/fasterxml/jackson/core/jackson-core/2.14.3/jackson-core-2.14.3.jar \
 && wget -q -P /opt/flink/lib/ \
    https://repo1.maven.org/maven2/com/fasterxml/jackson/core/jackson-annotations/2.14.3/jackson-annotations-2.14.3.jar

# Confluent Schema Registry client + transitive deps (kafka-clients, guava, etc.)
# Uses Maven to resolve the full closure rather than chasing individual JARs
COPY confluent-deps-pom.xml /tmp/confluent-deps-pom.xml
USER root
RUN apt-get update -qq && apt-get install -y -q --no-install-recommends maven \
 && mvn -f /tmp/confluent-deps-pom.xml dependency:copy-dependencies \
      -DoutputDirectory=/opt/flink/lib \
      -Dmdep.useRepositoryLayout=false \
      -q \
 && apt-get purge -y maven && apt-get autoremove -y -q \
 && rm -rf /root/.m2 /tmp/confluent-deps-pom.xml /var/lib/apt/lists/*
USER flink
