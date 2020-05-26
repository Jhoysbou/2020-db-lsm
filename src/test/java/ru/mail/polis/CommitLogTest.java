package ru.mail.polis;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import ru.mail.polis.jhoysbou.commitlog.CommitLog;
import ru.mail.polis.jhoysbou.commitlog.LogRecord;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class CommitLogTest extends BasicTest{

    @Test
    void test(@TempDir File data) throws IOException {
        CommitLog commitLog = new CommitLog(data);
        LogRecord logRecord = new LogRecord(randomKey(), randomValue(), (byte) 0);
        commitLog.log(randomKey(), randomValue(), (byte) 0);
        Iterator<LogRecord> iterator = commitLog.provideData();
        assertEquals(iterator.next(), logRecord);

    }
}
