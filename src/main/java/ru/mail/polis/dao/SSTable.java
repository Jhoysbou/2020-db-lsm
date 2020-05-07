package ru.mail.polis.dao;

import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Iterator;

public class SSTable implements Table {
    private final FileChannel channel;
    private final int size;
    private final long indexStart;
    private final long indexEnd;

    SSTable(@NotNull final File file) {
        throw new UnsupportedOperationException();
    }

    @NotNull
    @Override
    public Iterator<Cell> iterator(@NotNull ByteBuffer from) throws IOException {
        return null;
    }

    @Override
    public void upsert(@NotNull ByteBuffer key, @NotNull ByteBuffer value) throws IOException {

    }

    @Override
    public void remove(@NotNull ByteBuffer key) throws IOException {

    }
}
