package ru.mail.polis.dao;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.TreeMap;

public class MemTable implements Table {
    private final SortedMap<ByteBuffer, Value> map = new TreeMap<>();

    @NotNull
    @Override
    public Iterator<Cell> iterator(@NotNull final ByteBuffer from) throws IOException {
        return map.tailMap(from)
                .entrySet()
                .stream()
                .map(element -> new Cell(element.getKey(), element.getValue()))
                .iterator();
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) throws IOException {
        map.put(key, new Value(System.currentTimeMillis(), value));
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) throws IOException {
        map.put(key, new Value(System.currentTimeMillis()));
    }

    @Override
    public int size() {
        return map.size();
    }

    @Override
    public long sizeInBytes() {
//        TODO: implement
    }
}
