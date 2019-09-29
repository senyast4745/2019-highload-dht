package ru.mail.polis.dao.senyast.model;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;

public final class Value implements Comparable<Value> {

    private final ByteBuffer data;
    private final long timestamp;
    private final boolean tombstone;

    public long getTimestamp() {
        return timestamp;
    }

    public ByteBuffer getData() {
        return data.asReadOnlyBuffer();
    }

    public static Value of(@NotNull final ByteBuffer data) {
        return new Value(data, System.currentTimeMillis(), false);
    }

    public static Value tombstone() {
        return new Value(null, System.currentTimeMillis(), true);
    }

    /**
     * Creating new Value with data.
     *
     * @param data writing data to value
     * @param timestamp time of creating
     * @param isDead boolean flag of consistency of data
     */
    public Value(final ByteBuffer data, final long timestamp, final boolean isDead) {
        this.data = data;
        this.timestamp = timestamp;
        this.tombstone = isDead;
    }

    public boolean isTombstone() {
        return tombstone;
    }

    @Override
    public int compareTo(@NotNull final Value o) {
        return -Long.compare(timestamp, o.timestamp);
    }
}
