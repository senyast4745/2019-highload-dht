package ru.mail.polis.dao.senyast.tables;

import com.google.common.collect.Iterators;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.dao.Iters;
import ru.mail.polis.dao.senyast.model.Cell;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class MemTablePool implements Table, Closeable {

    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    private volatile MemTable current;
    private NavigableMap<Integer, MemTable> pendingFlush;
    private BlockingQueue<TableToFlush> flushQueue;

    private final long memFlushThreshHold;

    private int generation;

    private AtomicInteger lastFlushedGeneration = new AtomicInteger(0);

    private AtomicBoolean stop = new AtomicBoolean(false);

    public MemTablePool(final long memFlushThreshHold, final int startGeneration, final int queueCapacity) {
        this.memFlushThreshHold = memFlushThreshHold;
        this.generation = startGeneration;
        this.pendingFlush = new ConcurrentSkipListMap<>();
        this.current = new MemTable(startGeneration);
        this.flushQueue = new ArrayBlockingQueue<>(queueCapacity);
    }

    @Override
    public long sizeInBytes() {
        lock.readLock().lock();
        try {
            long size = current.sizeInBytes();
            size += pendingFlush.values().stream().mapToLong(MemTable::sizeInBytes).sum();
            return size;
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public Iterator<Cell> iterator(@NotNull ByteBuffer from) throws IOException {

        lock.readLock().lock();
        final List<Iterator<Cell>> list;

        try {
            list = new ArrayList<>(pendingFlush.size() + 1);
            for (final Table fileChannelTable : pendingFlush.values()) {
                list.add(fileChannelTable.iterator(from));
            }
            final Iterator<Cell> memoryIterator = current.iterator(from);
            list.add(memoryIterator);
        } finally {
            lock.readLock().unlock();
        }

        //noinspection UnstableApiUsage
        return Iters.collapseEquals(Iterators.mergeSorted(list, Cell.COMPARATOR),
                Cell::getKey);
    }

    @Override
    public void upsert(@NotNull ByteBuffer key, @NotNull ByteBuffer value) {
        if (stop.get()) {
            throw new IllegalStateException("Database closed");
        }
        current.upsert(key, value);
        syncAddToFlush();
    }

    @Override
    public void remove(@NotNull ByteBuffer key) {
        if (stop.get()) {
            throw new IllegalStateException("Database closed");
        }
        current.remove(key);
        syncAddToFlush();
    }

    public TableToFlush toFlush() throws InterruptedException {
        return flushQueue.take();
    }

    private void updateCurrentFlushGeneration(int generation) {
        if (generation > lastFlushedGeneration.get()) {
            lastFlushedGeneration.set(generation);
        }
    }

    public void flushed(final int generation) {
        lock.writeLock().lock();
        try {
            pendingFlush.remove(generation);
        } finally {
            lock.writeLock().unlock();
        }

        updateCurrentFlushGeneration(generation);
    }

    public AtomicInteger getLastFlushedGeneration() {
        return lastFlushedGeneration;
    }

    private void syncAddToFlush() {
        if (current.sizeInBytes() > memFlushThreshHold) {
            lock.writeLock().lock();
            TableToFlush toFlush = null;
            try {
                if (current.sizeInBytes() > memFlushThreshHold) {
                    toFlush = new TableToFlush(current, generation);
                    pendingFlush.put(generation, current);
                    generation++;
                    current = new MemTable(generation);
                }
            } finally {
                lock.writeLock().unlock();
            }
            if (toFlush != null) {
                try {

                    flushQueue.put(toFlush);
                } catch (InterruptedException e) {
                    System.out.println("Thread interrupted");
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    @Override
    public void close() {
        if (!stop.compareAndSet(false, true)) {
            System.out.println("Stopped");
            return;
        }
        lock.writeLock().lock();
        final TableToFlush toFlush;
        try {
            pendingFlush.put(generation, current);
            toFlush = new TableToFlush(current, generation, true);
        } finally {
            lock.writeLock().unlock();
        }
        try {
            flushQueue.put(toFlush);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
