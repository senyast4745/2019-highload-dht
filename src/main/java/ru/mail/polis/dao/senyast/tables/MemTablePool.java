package ru.mail.polis.dao.senyast.tables;

import com.google.common.collect.Iterators;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
    private final NavigableMap<Integer, MemTable> pendingFlush;
    private final BlockingQueue<TableToFlush> flushQueue;

    private final long memFlushThreshHold;

    private int generation;

    private final AtomicInteger lastFlushedGeneration = new AtomicInteger(0);

    private final AtomicBoolean stop = new AtomicBoolean(false);

    private final Logger log = LoggerFactory.getLogger(MemTablePool.class);

    /**
     * Class to multithreading flush.
     *
     * @param memFlushThreshHold threshold at which we flush data to disk
     * @param startGeneration    start generation
     * @param queueCapacity      flush queue capacity
     */
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
    public Iterator<Cell> iterator(@NotNull final ByteBuffer from) throws IOException {

        final List<Iterator<Cell>> list;

        lock.readLock().lock();
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
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) {
        if (stop.get()) {
            throw new IllegalStateException("Database closed");
        }
        current.upsert(key, value);
        syncAddToFlush();
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) {
        if (stop.get()) {
            throw new IllegalStateException("Database closed");
        }
        current.remove(key);
        syncAddToFlush();
    }

    public TableToFlush toFlush() throws InterruptedException {
        return flushQueue.take();
    }

    private void updateCurrentFlushGeneration(final int generation) {
        if (generation > lastFlushedGeneration.get()) {
            lastFlushedGeneration.set(generation);
        }
    }

    /**
     * Callback method to inform about flush bu "Flusher thread".
     *
     * @param generation number of flushed generation
     */
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
            TableToFlush toFlush = null;
            lock.writeLock().lock();
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
                    log.info("Thread interrupted");
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    @Override
    public void close() {
        if (!stop.compareAndSet(false, true)) {
            log.info("Stopped");
            return;
        }
        final TableToFlush toFlush;
        lock.writeLock().lock();
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
