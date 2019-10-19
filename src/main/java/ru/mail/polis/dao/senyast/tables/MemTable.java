package ru.mail.polis.dao.senyast.tables;

import com.google.common.collect.Iterators;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.dao.senyast.model.Cell;
import ru.mail.polis.dao.senyast.model.Value;

import javax.annotation.concurrent.ThreadSafe;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

@ThreadSafe
public class MemTable implements Table {
    private final NavigableMap<ByteBuffer, Value> map;
    private AtomicLong tableSize = new AtomicLong();
    private final long generation;

    MemTable(final long generation) {
        this.generation = generation;
        this.map = new ConcurrentSkipListMap<>();
    }

    /**
     * Get data iterator from memory table.
     *
     * @param from key to find data
     * @return data iterator
     */
    @Override
    public final Iterator<Cell> iterator(@NotNull final ByteBuffer from) {
        return Iterators.transform(
                map.tailMap(from).entrySet().iterator(),
                entry -> {
                    assert entry != null;
                    return new Cell(entry.getKey(), entry.getValue(), generation);
                });
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) {
        final Value prev = map.put(key, Value.of(value));
        if (prev == null) {
            tableSize.addAndGet(key.remaining() + value.remaining());
        } else if (prev.isTombstone()) {
            tableSize.addAndGet(value.remaining());
        } else {
            tableSize.addAndGet(value.remaining() - prev.getData().remaining());
        }
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) {
        final Value prev = map.put(key, Value.tombstone());
        if (prev == null) {
            tableSize.addAndGet(key.remaining());
        } else if (!prev.isTombstone()) {
            tableSize.addAndGet(-prev.getData().remaining());
        }
    }

    @Override
    public long sizeInBytes() {
        return tableSize.get();
    }
}