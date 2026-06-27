package io.tibobit.orderbook.model;

public enum OrderBookEventType {
    UPDATE("update"),
    SNAPSHOT("snapshot");

    private final String value;

    OrderBookEventType(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    // Optional: convert from string back to enum
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
