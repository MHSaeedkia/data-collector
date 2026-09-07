package io.tibobit.normalizer.typevalidate;

import java.nio.charset.StandardCharsets;
import java.util.regex.Pattern;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.TransactionNamingStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import io.tibobit.normalizer.checkpoint.CheckpointingConfigurer;
import io.tibobit.normalizer.kafka.PatternTopicSelector;
import io.tibobit.normalizer.lookup.RefreshingLookup;
import io.tibobit.normalizer.model.ControlCommand;
import io.tibobit.normalizer.model.RawOrderBookEvent;
import io.tibobit.normalizer.model.RejectedOrderBookEvent;
import io.tibobit.normalizer.serde.ControlCommandSerializer;
import io.tibobit.normalizer.serde.RawOrderBookEventDeserializer;
import io.tibobit.normalizer.serde.RawOrderBookEventSerializer;
import io.tibobit.normalizer.serde.RejectedOrderBookEventSerializer;

/**
 * Job 2 entry point: type validation.
 *
 * Pipeline: Kafka input ex{id}-p{id}-raw-flink (RawOrderBookEvent, subject
 * raw-order-book-event) -> source (regex) -> keyBy(exchange_id, pair_id) ->
 * TypeValidateFunction (snapshot/update sequence rules) -> main Kafka output
 * ex{id}-p{id}-type-validated-raw-flink (subject raw-order-book-event) ->
 * reject Kafka output ex{id}-p{id}-rejected-flink (subject
 * rejected-order-book-event)
 */
public class TypeValidatorJob {

    private static final Pattern INPUT_TOPIC_PATTERN = Pattern.compile("ex[0-9]+-p[0-9]+-raw-flink");
    private static final Pattern VALIDATED_TOPIC_PATTERN =
            Pattern.compile("ex[0-9]+-p[0-9]+-type-validated-raw-flink");
    private static final Pattern REJECTED_TOPIC_PATTERN = Pattern.compile("ex[0-9]+-p[0-9]+-rejected-flink");

    public static void main(String[] args) throws Exception {
        String bootstrapServers = getEnv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092");
        String groupId = getEnv("KAFKA_GROUP_ID", "normalizer-type-validator");
        String schemaRegistryUrl = getEnv("SCHEMA_REGISTRY_URL", "http://schema-registry:8082");
        // How long to wait before re-asking NiFi for a snapshot that never arrived. One
        // request per episode is not enough on its own: if the command is lost or nothing
        // is consuming control-plane, the market stays dark until the job restarts.
        long snapshotRetryMs = Long.parseLong(getEnv("SNAPSHOT_RETRY_MS",
                String.valueOf(TypeValidateFunction.DEFAULT_SNAPSHOT_RETRY_MS)));

        // Staleness watch list. The thresholds themselves are per market and live in
        // exchange_markets.staleness_threshold_seconds — these three only say how to
        // reach the DB, and REFRESH_INTERVAL_MS how often to re-read the list so a
        // threshold edit or an unsubscribe lands without resubmitting the job.
        //
        // REFRESH_INTERVAL_MS must stay WELL BELOW the smallest
        // staleness_threshold_seconds in the table. Both clocks start at the same
        // moment on an unsubscribe — the feed stops, and the silence deadline is one
        // threshold away — so at 60s against a 60s threshold it is a coin flip whether
        // the timer finds the market still listed. Losing that race is not fatal (the
        // stale branch empties the book too, and NiFi must ignore a request for an
        // unsubscribed market), but it sends a snapshot_request nobody should act on.
        // A quarter of the smallest threshold keeps the unsubscribe on the clean path.
        String postgresUrl = getEnv("POSTGRES_URL", "jdbc:postgresql://postgres:5432/markets");
        String postgresUser = getEnv("POSTGRES_USER", "postgres");
        String postgresPassword = getEnv("POSTGRES_PASSWORD", "postgres");
        long refreshIntervalMs = Long.parseLong(getEnv("REFRESH_INTERVAL_MS", "15000"));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        CheckpointingConfigurer.configure(env);

        KafkaSource<RawOrderBookEvent> source = KafkaSource.<RawOrderBookEvent>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopicPattern(INPUT_TOPIC_PATTERN)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.latest())
                // job 1's sink is now EXACTLY_ONCE/transactional; without this, a
                // read_uncommitted consumer (Kafka's default) can see records from a
                // transaction that later aborts — silent corruption one hop earlier than the
                // replay problem checkpointing exists to close.
                .setProperty("isolation.level", "read_committed")
                .setValueOnlyDeserializer(new RawOrderBookEventDeserializer(schemaRegistryUrl))
                .build();

        // The watch list rides along inside the operator: it is only ever read by key,
        // so there is no second stream and no extra shuffle. A market missing from it is
        // simply not watched for silence.
        RefreshingLookup<String, WatchedMarket> watched = new RefreshingLookup<>(
                new StalenessThresholdLoader(postgresUrl, postgresUser, postgresPassword),
                refreshIntervalMs);

        SingleOutputStreamOperator<RawOrderBookEvent> validated = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "raw-flink-source")
                .uid("raw-flink-source")
                .keyBy(new ExchangePairKey())
                .process(new TypeValidateFunction(snapshotRetryMs, watched))
                .name("type-validate")
                .uid("type-validate");

        // Valid events -> ex{id}-p{id}-type-validated-raw-flink (same shared
        // raw-order-book-event schema).
        validated.sinkTo(KafkaSink.<RawOrderBookEvent>builder()
                .setBootstrapServers(bootstrapServers)
                // EXACTLY_ONCE, POOLING and the pattern-aware topic selector: see
                // PairExtractorJob's sink comment for why (same reasoning, every sink).
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setTransactionalIdPrefix("job2-type-validated")
                .setTransactionNamingStrategy(TransactionNamingStrategy.POOLING)
                .setProperty("acks", "all")
                .setProperty("enable.idempotence", "true")
                .setProperty("retries", "2147483647")
                .setProperty("delivery.timeout.ms", "120000")
                .setProperty("transaction.timeout.ms", "600000")
                .setRecordSerializer(KafkaRecordSerializationSchema.<RawOrderBookEvent>builder()
                        .setTopicSelector(new PatternTopicSelector<RawOrderBookEvent>(VALIDATED_TOPIC_PATTERN) {
                            @Override
                            public String apply(RawOrderBookEvent event) {
                                return "ex" + event.getExchangeId() + "-p" + event.getPairId()
                                        + "-type-validated-raw-flink";
                            }
                        })
                        .setValueSerializationSchema(
                                new RawOrderBookEventSerializer(schemaRegistryUrl))
                        .build())
                .build())
                .name("type-validated-sink")
                .uid("type-validated-sink");

        // Rejects -> dead-letter ex{id}-p{id}-rejected-flink (subject
        // rejected-order-book-event).
        DataStream<RejectedOrderBookEvent> rejected = validated.getSideOutput(TypeValidateFunction.REJECTED);
        rejected.sinkTo(KafkaSink.<RejectedOrderBookEvent>builder()
                .setBootstrapServers(bootstrapServers)
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setTransactionalIdPrefix("job2-rejected")
                .setTransactionNamingStrategy(TransactionNamingStrategy.POOLING)
                .setProperty("acks", "all")
                .setProperty("enable.idempotence", "true")
                .setProperty("retries", "2147483647")
                .setProperty("delivery.timeout.ms", "120000")
                .setProperty("transaction.timeout.ms", "600000")
                .setRecordSerializer(KafkaRecordSerializationSchema.<RejectedOrderBookEvent>builder()
                        .setTopicSelector(new PatternTopicSelector<RejectedOrderBookEvent>(REJECTED_TOPIC_PATTERN) {
                            @Override
                            public String apply(RejectedOrderBookEvent rejection) {
                                return "ex" + rejection.getEvent().getExchangeId()
                                        + "-p" + rejection.getEvent().getPairId() + "-rejected-flink";
                            }
                        })
                        .setValueSerializationSchema(
                                new RejectedOrderBookEventSerializer(schemaRegistryUrl))
                        .build())
                .build())
                .name("rejected-sink")
                .uid("rejected-sink");

        // Control-plane -> shared control-plane topic, consumed by NiFi to trigger a fresh
        // snapshot. Deliberately left OFF EXACTLY_ONCE/transactions: NiFi consumes this topic and
        // would otherwise need read_committed too, and a resend of a snapshot request is harmless
        // (job 2 re-asks itself if the first one is lost), so there is nothing here worth trading
        // immediacy for.
        DataStream<ControlCommand> controlCommands = validated.getSideOutput(TypeValidateFunction.CONTROL);
        controlCommands.sinkTo(KafkaSink.<ControlCommand>builder()
                .setBootstrapServers(bootstrapServers)
                .setProperty("acks", "all")
                .setProperty("enable.idempotence", "true")
                .setProperty("retries", "2147483647")
                .setProperty("delivery.timeout.ms", "120000")
                .setRecordSerializer(KafkaRecordSerializationSchema.<ControlCommand>builder()
                        .setTopic("control-plane")
                        .setKeySerializationSchema(cmd
                                -> (cmd.getExchangeId() + "|" + cmd.getPairId())
                                .getBytes(StandardCharsets.UTF_8))
                        .setValueSerializationSchema(new ControlCommandSerializer(schemaRegistryUrl))
                        .build())
                .build())
                .name("control-plane-sink")
                .uid("control-plane-sink");

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
