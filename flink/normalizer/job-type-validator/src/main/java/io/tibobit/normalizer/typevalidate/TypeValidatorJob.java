package io.tibobit.normalizer.typevalidate;

import io.tibobit.normalizer.model.RawOrderBookEvent;
import io.tibobit.normalizer.model.RejectedOrderBookEvent;
import io.tibobit.normalizer.serde.RawOrderBookEventDeserializer;
import io.tibobit.normalizer.serde.RawOrderBookEventSerializer;
import io.tibobit.normalizer.serde.RejectedOrderBookEventSerializer;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.TopicSelector;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.regex.Pattern;

/**
 * Job 2 entry point: type validation.
 *
 * Pipeline:
 *   Kafka input  ex{id}-p{id}-raw-flink        (RawOrderBookEvent, subject raw-order-book-event)
 *     -> source (regex) -> keyBy(exchange_id, pair_id)
 *     -> TypeValidateFunction (snapshot/update sequence rules)
 *     -> main   Kafka output  ex{id}-p{id}-type-validated-raw-flink  (subject raw-order-book-event)
 *     -> reject Kafka output  ex{id}-p{id}-rejected-flink            (subject rejected-order-book-event)
 */
public class TypeValidatorJob {

    private static final Pattern INPUT_TOPIC_PATTERN = Pattern.compile("ex[0-9]+-p[0-9]+-raw-flink");

    public static void main(String[] args) throws Exception {
        String bootstrapServers = getEnv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092");
        String groupId = getEnv("KAFKA_GROUP_ID", "normalizer-type-validator");
        String schemaRegistryUrl = getEnv("SCHEMA_REGISTRY_URL", "http://schema-registry:8082");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<RawOrderBookEvent> source = KafkaSource.<RawOrderBookEvent>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopicPattern(INPUT_TOPIC_PATTERN)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new RawOrderBookEventDeserializer(schemaRegistryUrl))
                .build();

        SingleOutputStreamOperator<RawOrderBookEvent> validated = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "raw-flink-source")
                .keyBy(new ExchangePairKey())
                .process(new TypeValidateFunction())
                .name("type-validate");

        // Valid events -> ex{id}-p{id}-type-validated-raw-flink (same shared raw-order-book-event schema).
        validated.sinkTo(KafkaSink.<RawOrderBookEvent>builder()
                        .setBootstrapServers(bootstrapServers)
                        .setRecordSerializer(KafkaRecordSerializationSchema.<RawOrderBookEvent>builder()
                                .setTopicSelector((TopicSelector<RawOrderBookEvent>) event ->
                                        "ex" + event.getExchangeId() + "-p" + event.getPairId()
                                                + "-type-validated-raw-flink")
                                .setValueSerializationSchema(new RawOrderBookEventSerializer(schemaRegistryUrl))
                                .build())
                        .build())
                .name("type-validated-sink");

        // Rejects -> dead-letter ex{id}-p{id}-rejected-flink (subject rejected-order-book-event).
        DataStream<RejectedOrderBookEvent> rejected = validated.getSideOutput(TypeValidateFunction.REJECTED);
        rejected.sinkTo(KafkaSink.<RejectedOrderBookEvent>builder()
                        .setBootstrapServers(bootstrapServers)
                        .setRecordSerializer(KafkaRecordSerializationSchema.<RejectedOrderBookEvent>builder()
                                .setTopicSelector((TopicSelector<RejectedOrderBookEvent>) rejection ->
                                        "ex" + rejection.getEvent().getExchangeId()
                                                + "-p" + rejection.getEvent().getPairId() + "-rejected-flink")
                                .setValueSerializationSchema(new RejectedOrderBookEventSerializer(schemaRegistryUrl))
                                .build())
                        .build())
                .name("rejected-sink");

        env.execute("normalizer-type-validator");
    }

    /** keyBy (exchange_id, pair_id) — named class, not a lambda (Flink key-type inference). */
    private static final class ExchangePairKey implements KeySelector<RawOrderBookEvent, String> {
        @Override
        public String getKey(RawOrderBookEvent event) {
            return event.getExchangeId() + "|" + event.getPairId();
        }
    }

    private static String getEnv(String key, String fallback) {
        String value = System.getenv(key);
        return (value == null || value.isBlank()) ? fallback : value;
    }
}
