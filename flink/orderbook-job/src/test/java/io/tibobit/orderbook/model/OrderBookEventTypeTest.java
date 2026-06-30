package io.tibobit.orderbook.model;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests {@link OrderBookEventType}'s wire mapping in isolation: {@code getValue}/{@code toString}
 * give the lowercase wire token, {@code fromValue} parses case-insensitively, and an unknown
 * token fails loudly. The deserializer exercises the happy path, but the case-insensitivity and
 * the reject-unknown contract are the enum's own and are pinned here directly.
 */
class OrderBookEventTypeTest {

    /**
     * Given each enum constant, When its value/toString is read, Then it is the lowercase wire
     * token. The output serializer relies on {@code @JsonValue} emitting these exact strings.
     */
    @Test
    @DisplayName("exposes the lowercase wire token")
    void exposesWireToken() {
        assertThat(OrderBookEventType.SNAPSHOT.getValue()).isEqualTo("snapshot");
        assertThat(OrderBookEventType.UPDATE.getValue()).isEqualTo("update");
        assertThat(OrderBookEventType.SNAPSHOT).hasToString("snapshot");
        assertThat(OrderBookEventType.UPDATE).hasToString("update");
    }

    /**
     * Given a wire token in any casing, When parsed via {@code fromValue}, Then it resolves to
     * the matching constant. NiFi/exchange producers are not guaranteed to use one casing, so the
     * mapping must tolerate "SNAPSHOT", "Update", etc.
     */
    @Test
    @DisplayName("parses tokens case-insensitively")
    void parsesCaseInsensitively() {
        assertThat(OrderBookEventType.fromValue("snapshot")).isEqualTo(OrderBookEventType.SNAPSHOT);
        assertThat(OrderBookEventType.fromValue("SNAPSHOT")).isEqualTo(OrderBookEventType.SNAPSHOT);
        assertThat(OrderBookEventType.fromValue("Update")).isEqualTo(OrderBookEventType.UPDATE);
    }

    /**
     * Given a token that is not a known event kind, When parsed, Then {@code fromValue} throws
     * {@link IllegalArgumentException} naming the bad value rather than returning null. The merger
     * branches on the type, so an unrecognized kind must fail fast at the boundary.
     */
    @Test
    @DisplayName("rejects an unknown token")
    void rejectsUnknownToken() {
        assertThatThrownBy(() -> OrderBookEventType.fromValue("delta"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unknown type: delta");
    }
}
