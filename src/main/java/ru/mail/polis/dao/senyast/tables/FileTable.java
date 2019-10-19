package ru.mail.polis.dao.senyast.tables;

import org.jetbrains.annotations.NotNull;
import ru.mail.polis.dao.senyast.model.Bytes;
import ru.mail.polis.dao.senyast.model.Cell;
import ru.mail.polis.dao.senyast.model.Value;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.LongBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class FileTable implements Table {
    private final int rows;
    private final LongBuffer offsets;
    private final ByteBuffer cells;
    private final long generation;
    private final File file;

    /**
     * Creates instance of FileTable and get data from file.
     *
     * @param file       to get data
     * @param generation of data
     * @throws IOException if was input or output errors
     */
    public FileTable(@NotNull final File file, final long generation) throws IOException {
        this.generation = generation;
        this.file = file;

        final long fileSize = file.length();
        final ByteBuffer mapped;
        try (FileChannel fc = FileChannel.open(file.toPath(), StandardOpenOption.READ)) {
            assert fileSize <= Integer.MAX_VALUE;
            mapped = fc.map(FileChannel.MapMode.READ_ONLY, 0L, fileSize).order(ByteOrder.BIG_ENDIAN);
        }
        final long rowsValue = mapped.getLong((int) (fileSize - Long.BYTES));
        assert rowsValue <= Integer.MAX_VALUE;
        this.rows = (int) rowsValue;

        final ByteBuffer offsetBuffer = mapped.duplicate();
        offsetBuffer.position(mapped.limit() - Long.BYTES * rows - Long.BYTES);
        offsetBuffer.limit(mapped.limit() - Long.BYTES);
        this.offsets = offsetBuffer.slice().asLongBuffer();

        final ByteBuffer cellBuffer = mapped.duplicate();
        cellBuffer.limit(offsetBuffer.position());
        this.cells = cellBuffer.slice();
    }

    /**
     * Write data to disk.
     *
     * @param cells data iterator to write
     * @param to    ile location
     * @throws IOException if was input or output errors
     */
    public static void writeToFile(@NotNull final Iterator<Cell> cells, @NotNull final File to)
            throws IOException {
        try (FileChannel fileChannel = FileChannel.open(
                to.toPath(), StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
            final List<Long> offsets = new ArrayList<>();
            long offset = 0;
            while (cells.hasNext()) {
                offsets.add(offset);

                final Cell cell = cells.next();

                final ByteBuffer key = cell.getKey();
                final int keySize = cell.getKey().remaining();
                fileChannel.write(Bytes.fromInt(keySize));
                offset += Integer.BYTES;
                final ByteBuffer keyDuplicate = key.duplicate();
                fileChannel.write(keyDuplicate);
                offset += keySize;

                final Value value = cell.getValue();

                if (value.isTombstone()) {
                    fileChannel.write(Bytes.fromLong(-cell.getValue().getTimestamp()));
                } else {
                    fileChannel.write(Bytes.fromLong(cell.getValue().getTimestamp()));
                }

                offset += Long.BYTES;
                if (!value.isTombstone()) {
                    final ByteBuffer valueData = value.getData();
                    final int valueSize = value.getData().remaining();
                    fileChannel.write(Bytes.fromInt(valueSize));
                    offset += Integer.BYTES;
                    fileChannel.write(valueData);
                    offset += valueSize;
                }
            }
            for (final Long anOffset : offsets) {
                fileChannel.write(Bytes.fromLong(anOffset));
            }

            fileChannel.write(Bytes.fromLong(offsets.size()));
        }
    }

    @Override
    public long sizeInBytes()
    {
        return 0;
    }

    @NotNull
    @Override
    public Iterator<Cell> iterator(@NotNull final ByteBuffer from) {
        return new Iterator<>() {
            int next = position(from);

            @Override
            public boolean hasNext()
            {
                return next < rows;
            }

            @Override
            public Cell next() {
                assert hasNext();
                return cellAt(next++);
            }
        };
    }

    @Override
    public void upsert(final @NotNull ByteBuffer key, final @NotNull ByteBuffer value) {
        throw new UnsupportedOperationException("");
    }

    @Override
    public void remove(final @NotNull ByteBuffer key) {
        throw new UnsupportedOperationException("");
    }

    private int position(final @NotNull ByteBuffer from) {
        int left = 0;
        int right = rows - 1;
        while (left <= right) {
            final int mid = left + (right - left) / 2;
            final int cmp = from.compareTo(keyAt(mid));
            if (cmp < 0) {
                right = mid - 1;
            } else if (cmp > 0) {
                left = mid + 1;
            } else {
                return mid;
            }
        }
        return left;
    }

    private ByteBuffer keyAt(final int i) {
        assert 0 <= i && i < rows;
        final long offset = offsets.get(i);
        assert offset <= Integer.MAX_VALUE;
        final int keySize = cells.getInt((int) offset);
        final ByteBuffer key = cells.duplicate();
        key.position((int) (offset + Integer.BYTES));
        key.limit(key.position() + keySize);
        return key.slice();
    }

    private Cell cellAt(final int i) {
        assert 0 <= i && i < rows;
        long offset = offsets.get(i);
        assert offset <= Integer.MAX_VALUE;

        final int keySize = cells.getInt((int) offset);
        offset += Integer.BYTES;
        final ByteBuffer key = cells.duplicate();
        key.position((int) offset);
        key.limit(key.position() + keySize);
        offset += keySize;

        final long timeStamp = cells.getLong((int) offset);
        offset += Long.BYTES;

        if (timeStamp < 0) {
            return new Cell(key.slice(), new Value(null, -timeStamp, true), generation);
        } else {
            final int valueSize = cells.getInt((int) offset);
            offset += Integer.BYTES;
            final ByteBuffer value = cells.duplicate();
            value.position((int) offset);
            value.limit(value.position() + valueSize)
                    .position((int) offset)
                    .limit((int) (offset + valueSize));
            return new Cell(key.slice(), new Value(value.slice(), timeStamp, false), generation);
        }
    }

    public File getFile() {
        return file;
    }
}
