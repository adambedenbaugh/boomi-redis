package com.boomi.connector.redis.connection;

/** A single Redis Stream entry as this connector models it: an id plus one plain string value. */
public final class RedisStreamMessage {

    private final String id;
    private final String value;

    public RedisStreamMessage(String id, String value) {
        this.id = id;
        this.value = value;
    }

    public String getId() {
        return id;
    }

    public String getValue() {
        return value;
    }
}
