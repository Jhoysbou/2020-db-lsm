package ru.mail.polis.jhoysbou;

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
    private final int shift;


    SSTable(@NotNull final File file) throws IOException {
        channel = FileChannel.open(file.toPath(), StandardOpenOption.READ);
        int fileSize = (int) channel.size();
        ByteBuffer offsetBuf = ByteBuffer.allocate(Integer.BYTES);
        channel.read(offsetBuf, fileSize - Integer.BYTES);
        size = offsetBuf.flip().getInt();
        shift = fileSize - Integer.BYTES * (size + 1);
    }

    static void serialize(final File file, final Iterator<Cell> elementsIter, final int amount) throws IOException {
        try (FileChannel fileChannel = FileChannel.open(file.toPath(), StandardOpenOption.WRITE)) {

            List<Integer> offsets = new ArrayList<>();
            int offset = 0;

            while (elementsIter.hasNext()) {
                Cell cell = elementsIter.next();
                ByteBuffer key = cell.getKey();
                int keySize = key.remaining();
                Value value = cell.getValue();

                offsets.add(offset);
                offset += keySize + Integer.BYTES * 2 + Long.BYTES;

                fileChannel.write(ByteBuffer.allocate(Integer.BYTES).putInt(keySize).flip());
                fileChannel.write(key);
                fileChannel.write(ByteBuffer.allocate(Long.BYTES).putLong(value.getTimestamp()).flip());

                if (value.isTombstone()) {
                    fileChannel.write(ByteBuffer.allocate(Integer.BYTES).putInt(-1).flip());
                } else {
                    ByteBuffer valueBuffer = value.getData();
                    int valueSize = valueBuffer.remaining();
                    fileChannel.write(ByteBuffer.allocate(Integer.BYTES).putInt(valueSize).flip());
                    fileChannel.write(valueBuffer);
                    offset += valueSize;
                }
            }

            for (Integer i : offsets) {
                fileChannel.write(ByteBuffer.allocate(Integer.BYTES).putInt(i).flip());
            }
            fileChannel.write(ByteBuffer.allocate(Integer.BYTES).putInt(amount).flip());
        }
    }

    private int getOffset(final int position) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
        channel.read(buffer, shift + position * Integer.BYTES);
        return buffer.flip().getInt();
    }

    private ByteBuffer getKey(final int position) throws IOException {
        int keyLengthOffset = getOffset(position);

        ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
        channel.read(buffer, keyLengthOffset);
        buffer = ByteBuffer.allocate(buffer.flip().getInt());
        channel.read(buffer, keyLengthOffset + Integer.BYTES);

        return buffer.flip();
    }

    private Cell getCell(final int position) throws IOException {
        int elementOffset = getOffset(position);

        final ByteBuffer key = getKey(position);

        elementOffset += Integer.BYTES + key.remaining();
        final ByteBuffer timestampBuf = ByteBuffer.allocate(Long.BYTES);
        channel.read(timestampBuf, elementOffset);

        final ByteBuffer valueSizeBuf = ByteBuffer.allocate(Integer.BYTES);
        channel.read(valueSizeBuf, elementOffset + Long.BYTES);
        final int valueSize = valueSizeBuf.flip().getInt();

        final Value value;
        if (valueSize == -1) {
            value = new Value(timestampBuf.flip().getLong());
        } else {
            final ByteBuffer valueBuf = ByteBuffer.allocate(valueSize);
            channel.read(valueBuf, elementOffset + Long.BYTES + Integer.BYTES);
            valueBuf.flip();
            value = new Value(timestampBuf.flip().getLong(), valueBuf);
        }

        return new Cell(key, value);
    }

    @NotNull
    @Override
    public Iterator<Cell> iterator(@NotNull final ByteBuffer from) throws IOException {
        return new Iterator<Cell>() {
            private int position = binarySearch(from.rewind());

            @Override
            public boolean hasNext() {
                return position < size;
            }

            @Override
            public Cell next() {
                try {
                    return getCell(position++);
                } catch (IOException e) {
                    throw new RuntimeException();
                }
            }
        };
    }

    private int binarySearch(final ByteBuffer key) throws IOException {
        int left = 0;
        int right = size - 1;
        while (left <= right) {
            final int mid = (left + right) / 2;
            final ByteBuffer midKey = getKey(mid);
            final int compareResult = midKey.compareTo(key);

            if (compareResult < 0) {
                left = mid + 1;
            } else if (compareResult > 0) {
                right = mid - 1;
            } else {
                return mid;
            }
        }

        return left;
    }


    @Override
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) {
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