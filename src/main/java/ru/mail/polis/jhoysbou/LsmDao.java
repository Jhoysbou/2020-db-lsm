package ru.mail.polis.jhoysbou;

import com.google.common.collect.Iterators;
import com.google.common.collect.UnmodifiableIterator;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.DAO;
import ru.mail.polis.Iters;
import ru.mail.polis.Record;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.stream.Stream;

public class LsmDao implements DAO {

    private static final Logger log = LoggerFactory.getLogger(LsmDao.class);
    private static final String SUFFIX = ".dat";
    private static final String TEMP = ".tmp";
    private final NavigableMap<Integer, Table> ssTables;
    @NotNull
    private final File storage;
    private final long flushThreshold;
    // Data
    private Table memTable;
    // State
    private int generation;

    /**
     * Simple LSM.
     *
     * @param storage        the directory to store data
     * @param flushThreshold maximum bytes to store in memory.
     *                       If a size of data gets larger, the program flushes it on drive.
     * @throws IOException when where are problems with getting list of files in storage directory
     */
    public LsmDao(
            final @NotNull File storage,
            final long flushThreshold) throws IOException {
        this.generation = 0;

        assert flushThreshold > 0L;
        this.flushThreshold = flushThreshold;
        this.storage = storage;
        this.memTable = new MemTable();
        this.ssTables = new TreeMap<>();

        try (Stream<Path> files = Files.list(storage.toPath())) {
            files.filter(path -> path.toString().endsWith(SUFFIX)).forEach(f -> {
                try {
                    final String name = f.getFileName().toString();
                    final int lastFlushedGeneration = Integer.parseInt(name.substring(0, name.indexOf(SUFFIX)));
                    this.generation = Math.max(this.generation, lastFlushedGeneration);
                    ssTables.put(lastFlushedGeneration, new SSTable(f.toFile()));
                } catch (IOException e) {
                    log.warn("IOException with .dat file:\n" + e.getMessage());
                }
            });
        }
    }

    @NotNull
    @Override
    public Iterator<Record> iterator(final @NotNull ByteBuffer from) throws IOException {
        final List<Iterator<Cell>> iters = new ArrayList<>(ssTables.size() + 1);
        iters.add(memTable.iterator(from));
        ssTables.descendingMap().values().forEach(t -> {
            try {
                iters.add(t.iterator(from));
            } catch (IOException e) {
                log.error("IOException in lsm iterator\n" + e);
                e.printStackTrace();
            }
        });
        // Sorted duplicates and tombstones
        final Iterator<Cell> merged = Iterators.mergeSorted(iters, Cell.COMPARATOR);
        // One cell per key
        final Iterator<Cell> fresh = Iters.collapseEquals(merged, Cell::getKey);
        // No tombstones
        final UnmodifiableIterator<Cell> alive = Iterators.filter(fresh, e -> !e.getValue().isTombstone());

        return Iterators.transform(alive, e -> Record.of(e.getKey(), e.getValue().getData()));
    }

    @Override
    public void upsert(final @NotNull ByteBuffer key, final @NotNull ByteBuffer value) throws IOException {
        memTable.upsert(key, value);
        if (memTable.sizeInBytes() > flushThreshold) {
            flush();
        }
    }

    @Override
    public void remove(final @NotNull ByteBuffer key) throws IOException {
        memTable.remove(key);
        if (memTable.sizeInBytes() > flushThreshold) {
            flush();
        }
    }

    private void flush() throws IOException {
        final File file = new File(storage, generation + TEMP);
        file.createNewFile();
        SSTable.serialize(
                file,
                memTable.iterator(ByteBuffer.allocate(0)),
                memTable.size()
        );
        final File dst = new File(storage, generation + SUFFIX);
        Files.move(file.toPath(), dst.toPath(), StandardCopyOption.ATOMIC_MOVE);

        memTable = new MemTable();
        ssTables.put(generation++, new SSTable(dst));
    }

    @Override
    public void close() throws IOException {
        if (memTable.size() > 0) {
            flush();
        }

        for (final Table t : ssTables.values()) {
            t.close();
        }
    }
}

