package org.zhuanyi.jraftdb.engine.log;

import org.zhuanyi.jraftdb.engine.utils.Slice;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 进行预写日志的写
 */
public class LogWriterImpl implements LogWriter {

    private final AtomicBoolean closed = new AtomicBoolean();

    @Override
    public boolean isClosed() {
        return closed.get();
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public void delete() throws IOException {

    }

    @Override
    public File getFile() {
        return null;
    }

    @Override
    public long getFileNumber() {
        return 0;
    }

    @Override
    public void addRecord(Slice record, boolean force) throws IOException {

    }
}
