package org.zhuanyi.jraftdb.engine.log;

import org.zhuanyi.jraftdb.engine.option.DbOptions;
import org.zhuanyi.jraftdb.engine.utils.file.FileChannelWritableFile;
import org.zhuanyi.jraftdb.engine.utils.file.MMapWritableFile;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;

/**
 * Log相关的factory
 */
public class LogFactory {

    public static LogRecordWriter createLogWriter(File file, long fileNumber)
            throws IOException {
        if (DbOptions.USE_MMAP) {
            return new LogRecordWriterImpl(new MMapWritableFile(file, fileNumber));
        } else {
            return new LogRecordWriterImpl(new FileChannelWritableFile(file, fileNumber));
        }
    }


    public static LogRecordReader createLogReader(FileChannel fileChannel, LogMonitor logMonitor, long initialOffset) {
        return new LogRecordReaderImpl(fileChannel, logMonitor, false, initialOffset);
    }

}
