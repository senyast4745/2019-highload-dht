package ru.mail.polis.dao.senyast.model;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public final class Value implements Comparable<Value> {

    private static final Value ABSENT = new Value(null, 0, State.ABSENT);
    private final ByteBuffer data;
    private final long timestamp;
    //    private final boolean tombstone;
//    private static final AtomicLong nano = new AtomicLong();
    private final State state;

    public long getTimestamp() {
        return timestamp;
    }

    public ByteBuffer getData() {
        return data.asReadOnlyBuffer();
    }

    public static Value of(@NotNull final ByteBuffer data) {
        return new Value(data, System.nanoTime(), State.PRESENT);
    }

    public static Value of(final long time, final ByteBuffer data) {
        return new Value(data.duplicate(), time, State.PRESENT);
    }

    public static Value tombstone() {
        return new Value(null, System.nanoTime(), State.ABSENT);
    }

    public static Value tombstone(final long time) {
        return new Value(null, time, State.REMOVED);
    }

    /**
     * Creating new Value with data.
     *
     * @param data      writing data to value
     * @param timestamp time of creating
     * @param state     flag of consistency of data
     */
    public Value(final ByteBuffer data, final long timestamp, final State state) {
        this.data = data;
        this.timestamp = timestamp;
        this.state = state;
    }

    /*        public boolean isTombstone() {
            return tombstone;
        }*/
    public State state() {
        return state;
    }

    @NotNull
    public static Value absent() {
        return ABSENT;
    }

    @Override
    public int compareTo(@NotNull final Value o) {
        return -Long.compare(timestamp, o.timestamp);
    }

    @NotNull
    public static Value merge(@NotNull final Collection<Value> values) {
        return values.stream()
                .filter(v -> v.state() != Value.State.ABSENT)
                .min(Value::compareTo)
                .orElseGet(Value::absent);
    }

    public enum State {
        PRESENT,
        REMOVED,
        ABSENT
    }
}
