package ru.mail.polis.dao.senyast.tables;

import org.jetbrains.annotations.NotNull;
import ru.mail.polis.dao.senyast.model.Cell;


import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

public interface Table {
    long sizeInBytes();

    Iterator<Cell> iterator(@NotNull ByteBuffer from) throws IOException;

    void upsert(
            @NotNull ByteBuffer key,
            @NotNull ByteBuffer value) throws IOException;

    void remove(@NotNull ByteBuffer key) throws IOException;

}
