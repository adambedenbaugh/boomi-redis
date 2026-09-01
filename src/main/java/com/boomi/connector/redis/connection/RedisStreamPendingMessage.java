package com.boomi.connector.redis.connection;

/** Metadata for one entry in a Stream consumer group's pending entries list (PEL). */
public final class RedisStreamPendingMessage {

    private final String id;
    private final long deliveryCount;

    public RedisStreamPendingMessage(String id, long deliveryCount) {
        this.id = id;
        this.deliveryCount = deliveryCount;
    }

    public String getId() {
        return id;
    }

    public long getDeliveryCount() {
        return deliveryCount;
    }
}
