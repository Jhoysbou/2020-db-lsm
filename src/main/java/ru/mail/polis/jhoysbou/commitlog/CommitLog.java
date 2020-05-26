package ru.mail.polis.jhoysbou.commitlog;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;

public class CommitLog {
    private static final String LOG_FILE_NAME = "commit_log.log";
    private static final Logger log = LoggerFactory.getLogger(CommitLog.class);
    private long size;
    private final FileOutputStream logFileOut;
    private final File logFile;

    public CommitLog(@NotNull final File storage) throws IOException {
        logFile = new File(storage, LOG_FILE_NAME);
        logFileOut = new FileOutputStream(logFile, true);
    }

    public Iterator<LogRecord> provideData() throws IOException {
        return new Iterator<LogRecord>() {
            final FileChannel channel = FileChannel.open(logFile.toPath(), StandardOpenOption.READ);
            long position = 0;

            @Override
            public boolean hasNext() {
                return false;
            }

            @Override
            public LogRecord next() {
                try {
                    ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
                    channel.read(buffer, position);
                    final int operationType = buffer.flip().getInt();

                    buffer = ByteBuffer.allocate(Integer.BYTES);
                    channel.read(buffer, position + 1);
                    final int keySize = buffer.flip().getInt();

                    buffer = ByteBuffer.allocate(keySize);
                    channel.read(buffer, position + 1 + Integer.BYTES);
                    final ByteBuffer key = buffer.flip();

                    buffer = ByteBuffer.allocate(Integer.BYTES);
                    channel.read(buffer, position + 1 + Integer.BYTES + keySize);
                    final int valueSize = buffer.flip().getInt();

                    final ByteBuffer value = ByteBuffer.allocate(valueSize);
                    channel.read(value, position + 1 + 2*Integer.BYTES + keySize);
                    return new LogRecord(key, value.flip(), operationType);

                } catch (IOException e) {
                    log.error("IOException");
                }
                return null;
            }
        };


    }

    public void log(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value, final int operationType) {
        final int keySize = key.remaining();
        final int valueSize = value.remaining();
        final int recordSize = 2 * Integer.BYTES + keySize + valueSize;
        size += recordSize;

        byte[] keyArray = new byte[keySize];
        key.get(keyArray);

        byte[] valueArray = new byte[valueSize];
        value.get(valueArray);
        try {
            logFileOut.write(operationType);

            logFileOut.write(keySize);
            logFileOut.write(keyArray);

            logFileOut.write(valueSize);
            logFileOut.write(valueArray);
        } catch (IOException e) {
            log.error("IOIOException");
        }
    }

    public long getSize() {
        return size;
    }
}
