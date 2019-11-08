package ru.mail.polis.dao.senyast.model;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public final class Value implements Comparable<Value> {

    private static final Value ABSENT = new Value(State.ABSENT, 0, null);

    private final long ts;
    private final ByteBuffer data;
    private final State state;
    private static final AtomicInteger nano = new AtomicInteger();
    private static final int FACTOR = 1_000_000;

    public Value(State state, final long ts, final ByteBuffer data) {
        assert ts >= 0;
        this.state = state;
        this.ts = ts;
        this.data = data;
    }

    /**
     * Create Value and put ts from nano.
     *
     * @param data data marked by ts
     * @return and go back
     */
    public static Value of(final ByteBuffer data) {
        return new Value(State.PRESENT, getMoment(), data.duplicate());
    }

    public static Value of(final long time, final ByteBuffer data) {
        return new Value(State.PRESENT, time, data.duplicate());
    }

    @NotNull
    public static Value absent() {
        return ABSENT;
    }

    public static Value tombstone() {
        return tombstone(getMoment());
    }

    public static Value tombstone(final long time) {
        return new Value(State.REMOVED, time, null);
    }

    public boolean isTombstone() {
        return data == null;
    }

    public ByteBuffer getData() {
        if (data == null) {
            throw new IllegalArgumentException("");
        }
        return data.asReadOnlyBuffer();
    }

    @Override
    public int compareTo(@NotNull final Value value) {
        return Long.compare(value.ts, ts);
    }

    public long getTimestamp() {
        return ts;
    }

    private static long getMoment() {
        final long time = System.currentTimeMillis() * FACTOR + nano.incrementAndGet();
        if (nano.get() > FACTOR) {
            nano.set(0);
        }
        return time;
    }

    public State state() {
        return state;
    }

    @Override
    public String toString() {
        return state.toString() + ", ts=" + ts;
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
/*
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
        if (data == null) {
            throw new IllegalArgumentException("");
        }
        return data.asReadOnlyBuffer();
    }

    public static Value of(@NotNull final ByteBuffer data) {
        return new Value(data, System.nanoTime(), State.PRESENT);
    }

    public static Value of(final long time, final ByteBuffer data) {
        return new Value(data.duplicate(), time, State.PRESENT);
    }

    public static Value tombstone() {
        return new Value(null, System.nanoTime(), State.REMOVED);
    }

    public static Value tombstone(final long time) {
        return new Value(null, time, State.REMOVED);
    }

    *//**
     * Creating new Value with data.
     *
     * @param data      writing data to value
     * @param timestamp time of creating
     * @param state     flag of consistency of data
     *//*
    public Value(final ByteBuffer data, final long timestamp, final State state) {
        this.data = data;
        this.timestamp = timestamp;
        this.state = state;
    }

    public boolean isTombstone() {
        return data == null;
    }

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
    }*/
}
