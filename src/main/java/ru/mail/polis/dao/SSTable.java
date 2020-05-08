package ru.mail.polis.dao;

import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


public class SSTable implements Table {
    private final FileChannel channel;
    private final int size;
    private final long shift;

    SSTable(@NotNull final File file) throws IOException {
        this.channel = FileChannel.open(file.toPath(), StandardOpenOption.READ);
        final long fileSize = channel.size();
        final ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
        channel.read(buffer, fileSize - Integer.BYTES);
        this.size = buffer.flip().getInt();
        shift = fileSize - Integer.BYTES * (size + 1);
    }

    public static void serialize(final File file, final Iterator<Cell> iterator, final int size) throws IOException {
        try (FileChannel fileChannel = FileChannel.open(file.toPath(), StandardOpenOption.WRITE)) {
            final List<Integer> offsets = new ArrayList<>();
            int offset = 0;

            while (iterator.hasNext()) {
                final Cell cell = iterator.next();
                final ByteBuffer key = cell.getKey();
                final Value value = cell.getValue();
                final int keySize = key.remaining();

                offsets.add(offset);
                offset += keySize + 2 * Integer.BYTES + Long.BYTES;

                fileChannel.write(ByteBuffer.allocate(Integer.BYTES).putInt(keySize).flip());
                fileChannel.write(key);
                fileChannel.write(ByteBuffer.allocate(Long.BYTES).putLong(value.getTimestamp()).flip());

                if (value.isTombstone()) {
                    fileChannel.write(ByteBuffer.allocate(Integer.BYTES).putInt(-1).flip());
                } else {
                    final ByteBuffer valueBuffer = value.getData();
                    final int valueSize = valueBuffer.remaining();
                    fileChannel.write(ByteBuffer.allocate(Integer.BYTES).putInt(valueSize).flip());
                    fileChannel.write(valueBuffer);
                    offset += valueSize;
                }

                for (final Integer i : offsets) {
                    fileChannel.write(ByteBuffer.allocate(Integer.BYTES).putInt(i).flip());
                }
                fileChannel.write(ByteBuffer.allocate(Integer.BYTES).putInt(size).flip());
            }
        }
    }

    private int binarySearch(final ByteBuffer from) throws IOException {
        int left = 0;
        int right = size - 1;

        while (left <= right) {
            final int mid = (left + right) / 2;
            final ByteBuffer currentKey = getKey(mid);
            final int comparisonResult = from.compareTo(currentKey);

            if (comparisonResult > 0) {
                right = mid - 1;
            } else if (comparisonResult < 0) {
                left = mid + 1;
            } else {
                return mid;
            }
        }

        return left;
    }

    private int getOffset(final int position) throws IOException {
        final ByteBuffer byteBuffer = ByteBuffer.allocate(Integer.BYTES);
        channel.read(byteBuffer, shift + position * Integer.BYTES);
        return byteBuffer.flip().getInt();
    }

    private ByteBuffer getKey(final int position) throws IOException {
        final int keyOffset = getOffset(position);

        ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
        channel.read(buffer, keyOffset);
        ByteBuffer keyBuffer = ByteBuffer.allocate(buffer.flip().getInt());
        channel.read(keyBuffer, keyOffset + Integer.BYTES);
        return keyBuffer.flip();
    }

    private Cell getCell(final int position) throws IOException {
        int offset = getOffset(position);

        ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
        channel.read(buffer, offset);
        final int keySize = buffer.flip().getInt();

        buffer = ByteBuffer.allocate(keySize);
        channel.read(buffer, offset + Integer.BYTES);
        final ByteBuffer key = buffer.flip();

        buffer = ByteBuffer.allocate(Long.BYTES);
        channel.read(buffer, offset + Integer.BYTES + keySize + Long.BYTES);
        final long timestamp = buffer.flip().getLong();

        if (timestamp >= 0) {
            buffer = ByteBuffer.allocate(Integer.BYTES);
            channel.read(buffer, offset + Integer.BYTES + keySize + Long.BYTES + Integer.BYTES);

            return new Cell(key, new Value(
                    timestamp,
                    buffer.flip().flip()));
        } else {
            return new Cell(key, new Value(timestamp));
        }
    }

    @NotNull
    @Override
    public Iterator<Cell> iterator(@NotNull ByteBuffer from) throws IOException {
        return new Iterator<>() {
            private int position = binarySearch(from);

            @Override
            public boolean hasNext() {
                return position < size;
            }

            @Override
            public Cell next() {
                try {
                    return getCell(position++);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }

    @Override
    public void upsert(@NotNull ByteBuffer key, @NotNull ByteBuffer value) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void remove(@NotNull ByteBuffer key) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public long sizeInBytes() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }
}
