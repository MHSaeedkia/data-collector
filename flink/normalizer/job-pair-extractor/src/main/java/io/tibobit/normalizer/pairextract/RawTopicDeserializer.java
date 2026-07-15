package io.tibobit.normalizer.pairextract;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Kafka record → RawExchangeMessage: the exchange id lives ONLY in the topic name
 * ({@code ex{id}-raw}), so a value-only deserializer isn't enough — this captures the topic.
 * The payload bytes pass through untouched.
 */
public class RawTopicDeserializer implements KafkaRecordDeserializationSchema<RawExchangeMessage> {

    private static final Pattern RAW_TOPIC = Pattern.compile("ex([0-9]+)-raw");

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<RawExchangeMessage> out)
            throws IOException {
        Matcher matcher = RAW_TOPIC.matcher(record.topic());
        if (!matcher.matches()) {
            return; // unreachable with the source's topic pattern, but never crash on it
        }
        out.collect(new RawExchangeMessage(Integer.parseInt(matcher.group(1)), record.value()));
    }

    @Override
    public TypeInformation<RawExchangeMessage> getProducedType() {
        return TypeInformation.of(RawExchangeMessage.class);
    }
}
