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
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class MemTablePool implements Table, Closeable {

    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    private volatile MemTable current;
    private NavigableMap<Integer, MemTable> pendingFlush;
    private BlockingQueue<TableToFlush> flushQueue;

    private final long memFlushThreshHold;

    private int generation;

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
    public Iterator<Cell> iterator(@NotNull ByteBuffer from) {
        lock.readLock().lock();
        final Iterator<Cell> cellIterator;
        final List<Iterator<Cell>> iterators;
        try {
            iterators = new ArrayList<>(pendingFlush.size() + 1);
            for (final MemTable table : this.pendingFlush.values()) {
                iterators.add(table.iterator(from));
            }

            iterators.add(current.iterator(from));
        } finally {
            lock.readLock().unlock();
        }
        //noinspection UnstableApiUsage
        cellIterator = Iters.collapseEquals(
                Iterators.mergeSorted(iterators, Cell.COMPARATOR),
                Cell::getKey
        );

        return Iterators.filter(
                cellIterator, cell -> {
                    assert cell != null;
                    return !cell.getValue().isTombstone();
                }
        );
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

    public void flushed(final int generation) {
        lock.writeLock().lock();
        try {
            pendingFlush.remove(generation);
        } finally {
            lock.writeLock().unlock();
        }

    }

    private void syncAddToFlush() {
        if (current.sizeInBytes() > memFlushThreshHold) {
            lock.writeLock().unlock();
            if (current.sizeInBytes() > memFlushThreshHold) {
                TableToFlush toFlush;
                try {
                    toFlush = new TableToFlush(current, generation);
                } finally {
                    lock.writeLock().unlock();
                }

                try {
                    flushQueue.put(toFlush);
                } catch (InterruptedException e) {
                    System.out.println("Thread interrupted");
                    Thread.currentThread().interrupt();
                }
                generation++;
                current = new MemTable(generation);
            }

        }
    }

    public synchronized int getGeneration() {
        return generation;
    }

    @Override
    public void close() throws IOException {
        if (!stop.compareAndSet(false, true)) {
            System.out.println("Stopped");
            return;
        }
        lock.writeLock().lock();
        final TableToFlush toFlush;
        try {
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
