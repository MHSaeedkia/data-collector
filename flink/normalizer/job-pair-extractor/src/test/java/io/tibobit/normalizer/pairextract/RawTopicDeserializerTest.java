package io.tibobit.normalizer.pairextract;

import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests {@link RawTopicDeserializer}: the exchange id exists ONLY in the topic name, so the
 * deserializer must capture it while passing the payload bytes through verbatim.
 */
class RawTopicDeserializerTest {

    private final RawTopicDeserializer deserializer = new RawTopicDeserializer();

    private static List<RawExchangeMessage> collect(RawTopicDeserializer d,
                                                    ConsumerRecord<byte[], byte[]> record)
            throws Exception {
        List<RawExchangeMessage> out = new ArrayList<>();
        d.deserialize(record, new Collector<>() {
            @Override
            public void collect(RawExchangeMessage msg) {
                out.add(msg);
            }

            @Override
            public void close() {
            }
        });
        return out;
    }

    /**
     * Given a record from ex6-raw, When deserialized, Then the exchange id 6 is parsed from
     * the topic name and the payload bytes are untouched.
     */
    @Test
    @DisplayName("captures the exchange id from the topic name")
    void capturesExchangeId() throws Exception {
        byte[] payload = "{\"type\":\"snapshot\"}".getBytes(StandardCharsets.UTF_8);

        List<RawExchangeMessage> out =
                collect(deserializer, new ConsumerRecord<>("ex6-raw", 0, 0L, null, payload));

        assertThat(out).hasSize(1);
        assertThat(out.get(0).getExchangeId()).isEqualTo(6);
        assertThat(out.get(0).getPayload()).isEqualTo(payload);
    }

    /**
     * Given a record from a topic that isn't ex{id}-raw (defensive — the source pattern should
     * prevent it), When deserialized, Then nothing is emitted and nothing crashes.
     */
    @Test
    @DisplayName("ignores records from non-raw topics")
    void ignoresForeignTopics() throws Exception {
        List<RawExchangeMessage> out = collect(deserializer,
                new ConsumerRecord<>("ex1-p2-asks", 0, 0L, null, new byte[] {1}));

        assertThat(out).isEmpty();
    }
}
