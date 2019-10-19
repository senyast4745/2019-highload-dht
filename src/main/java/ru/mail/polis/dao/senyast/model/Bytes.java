package ru.mail.polis.dao.senyast.model;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;

public final class Bytes {

    private Bytes(){}

    public static ByteBuffer fromInt(final int value) {
        final ByteBuffer result = ByteBuffer.allocate(Integer.BYTES);
        return result.putInt(value).rewind();
    }

    public static ByteBuffer fromLong(final long value) {
        final ByteBuffer result = ByteBuffer.allocate(Long.BYTES);
        return result.putLong(value).rewind();
    }

    /**
     * Convert byte buffer to byte array.
     *
     * @param buffer byte buffer to convert to byte array
     * @return bytes array
     */
    public static byte[] toArray(@NotNull final ByteBuffer buffer) {
        final ByteBuffer duplicate = buffer.duplicate();
        final byte[] array = new byte[duplicate.remaining()];
        duplicate.get(array);
        return array;
    }
}
