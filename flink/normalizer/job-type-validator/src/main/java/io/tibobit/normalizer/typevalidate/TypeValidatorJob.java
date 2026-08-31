package io.tibobit.normalizer.typevalidate;

import io.tibobit.normalizer.model.RawOrderBookEvent;
import io.tibobit.normalizer.model.RejectedOrderBookEvent;
import io.tibobit.normalizer.serde.RawOrderBookEventDeserializer;
import io.tibobit.normalizer.serde.RawOrderBookEventSerializer;
import io.tibobit.normalizer.serde.RejectedOrderBookEventSerializer;
import io.tibobit.normalizer.checkpointingConfigurer.CheckpointingConfigurer;
import io.tibobit.normalizer.model.ControlCommand;
import io.tibobit.normalizer.serde.ControlCommandSerializer;

import java.nio.charset.StandardCharsets;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.connector.base.DeliveryGuarantee;
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
 * Kafka input ex{id}-p{id}-raw-flink (RawOrderBookEvent, subject
 * raw-order-book-event)
 * -> source (regex) -> keyBy(exchange_id, pair_id)
 * -> TypeValidateFunction (snapshot/update sequence rules)
 * -> main Kafka output ex{id}-p{id}-type-validated-raw-flink (subject
 * raw-order-book-event)
 * -> reject Kafka output ex{id}-p{id}-rejected-flink (subject
 * rejected-order-book-event)
 */
public class TypeValidatorJob {

        private static final Pattern INPUT_TOPIC_PATTERN = Pattern.compile("ex[0-9]+-p[0-9]+-raw-flink");

        public static void main(String[] args) throws Exception {
                String bootstrapServers = getEnv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092");
                String groupId = getEnv("KAFKA_GROUP_ID", "normalizer-type-validator");
                String schemaRegistryUrl = getEnv("SCHEMA_REGISTRY_URL", "http://schema-registry:8082");
                // How long to wait before re-asking NiFi for a snapshot that never arrived. One
                // request per episode is not enough on its own: if the command is lost or nothing
                // is consuming control-plane, the market stays dark until the job restarts.
                long snapshotRetryMs = Long.parseLong(getEnv("SNAPSHOT_RETRY_MS",
                                String.valueOf(TypeValidateFunction.DEFAULT_SNAPSHOT_RETRY_MS)));

                StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
                CheckpointingConfigurer.configure(env); 
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
                                .process(new TypeValidateFunction(snapshotRetryMs))
                                .name("type-validate");

                // Valid events -> ex{id}-p{id}-type-validated-raw-flink (same shared
                // raw-order-book-event schema).
                validated.sinkTo(KafkaSink.<RawOrderBookEvent>builder()
                                .setBootstrapServers(bootstrapServers)
                                // Without checkpointing, DeliveryGuarantee is NONE and a broker-side
                                // failure drops records silently. Idempotence is the load-bearing one:
                                // plain retries can reorder writes, which corrupts the book downstream.
                                .setProperty("acks", "all")
                                .setProperty("enable.idempotence", "true")
                                .setProperty("retries", "2147483647")
                                .setProperty("delivery.timeout.ms", "120000")
                                .setRecordSerializer(KafkaRecordSerializationSchema.<RawOrderBookEvent>builder()
                                                .setTopicSelector((TopicSelector<RawOrderBookEvent>) event -> "ex"
                                                                + event.getExchangeId() + "-p" + event.getPairId()
                                                                + "-type-validated-raw-flink")
                                                .setValueSerializationSchema(
                                                                new RawOrderBookEventSerializer(schemaRegistryUrl))
                                                .build())
                                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                                .setTransactionalIdPrefix("job2-type-validated")
                                .build())
                                .name("type-validated-sink");

                // Rejects -> dead-letter ex{id}-p{id}-rejected-flink (subject
                // rejected-order-book-event).
                DataStream<RejectedOrderBookEvent> rejected = validated.getSideOutput(TypeValidateFunction.REJECTED);
                rejected.sinkTo(KafkaSink.<RejectedOrderBookEvent>builder()
                                .setBootstrapServers(bootstrapServers)
                                // Without checkpointing, DeliveryGuarantee is NONE and a broker-side
                                // failure drops records silently. Idempotence is the load-bearing one:
                                // plain retries can reorder writes, which corrupts the book downstream.
                                .setProperty("acks", "all")
                                .setProperty("enable.idempotence", "true")
                                .setProperty("retries", "2147483647")
                                .setProperty("delivery.timeout.ms", "120000")
                                .setRecordSerializer(KafkaRecordSerializationSchema.<RejectedOrderBookEvent>builder()
                                                .setTopicSelector(
                                                                (TopicSelector<RejectedOrderBookEvent>) rejection -> "ex"
                                                                                + rejection.getEvent().getExchangeId()
                                                                                + "-p"
                                                                                + rejection.getEvent().getPairId()
                                                                                + "-rejected-flink")
                                                .setValueSerializationSchema(
                                                                new RejectedOrderBookEventSerializer(schemaRegistryUrl))
                                                .build())
                                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                                .setTransactionalIdPrefix("job2-rejected")
                                .build())
                                .name("rejected-sink");

                // Control-plane -> shared control-plane topic, consumed by NiFi to trigger a
                // fresh snapshot.
                DataStream<ControlCommand> controlCommands = validated.getSideOutput(TypeValidateFunction.CONTROL);
                controlCommands.sinkTo(KafkaSink.<ControlCommand>builder()
                .setBootstrapServers(bootstrapServers)
                // Without checkpointing, DeliveryGuarantee is NONE and a broker-side
                // failure drops records silently. Idempotence is the load-bearing one:
                // plain retries can reorder writes, which corrupts the book downstream.
                .setProperty("acks", "all")
                .setProperty("enable.idempotence", "true")
                .setProperty("retries", "2147483647")
                .setProperty("delivery.timeout.ms", "120000")
                .setRecordSerializer(KafkaRecordSerializationSchema.<ControlCommand>builder()
                        .setTopic("control-plane")
                        .setKeySerializationSchema(cmd ->
                                (cmd.getExchangeId() + "|" + cmd.getPairId())
                                        .getBytes(StandardCharsets.UTF_8))
                        .setValueSerializationSchema(new ControlCommandSerializer(schemaRegistryUrl))
                        .build())
                .build())
        .name("control-plane-sink");

                env.execute("normalizer-type-validator");
        }

        /**
         * keyBy (exchange_id, pair_id) — named class, not a lambda (Flink key-type
         * inference).
         */
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
