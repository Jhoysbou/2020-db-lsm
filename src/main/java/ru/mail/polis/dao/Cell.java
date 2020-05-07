package ru.mail.polis.dao;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.util.Comparator;

final class Cell {
    static final Comparator<Cell> COMPARATOR =
            Comparator.comparing(Cell::getKey)
                    .thenComparing(Cell::getValue);

    @NotNull
    private final ByteBuffer key;
    @NotNull
    private final Value value;

    Cell(
            @NotNull ByteBuffer key,
            @NotNull Value value) {
        this.key = key;
        this.value = value;
    }


    @NotNull
    public ByteBuffer getKey() {
        return key;
    }

    @NotNull
    public Value getValue() {
        return value;
    }
}
