package io.tibobit.orderbook.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

public enum OrderBookEventType {
    UPDATE("update"),
    SNAPSHOT("snapshot");

    private final String value;

    OrderBookEventType(String value) {
        this.value = value;
    }

    @JsonValue
    public String getValue() {
        return value;
    }

    // Optional: convert from string back to enum
    @JsonCreator
    public static OrderBookEventType fromValue(String value) {
        for (OrderBookEventType type : OrderBookEventType.values()) {
            if (type.value.equalsIgnoreCase(value)) {
                return type;
            }
        }
        throw new IllegalArgumentException("Unknown type: " + value);
    }

    @Override
    public String toString() {
        return value;
    }
}
