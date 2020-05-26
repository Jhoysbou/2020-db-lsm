package ru.mail.polis.jhoysbou.commitlog;

import java.nio.ByteBuffer;

public class LogRecord {
    private ByteBuffer key;
    private ByteBuffer value;
    private int operationType;

    public LogRecord(ByteBuffer key, ByteBuffer value, int operationType) {
        this.key = key;
        this.value = value;
        this.operationType = operationType;
    }

    public ByteBuffer getKey() {
        return key;
    }

    public ByteBuffer getValue() {
        return value;
    }

    public int getOperationType() {
        return operationType;
    }
}
