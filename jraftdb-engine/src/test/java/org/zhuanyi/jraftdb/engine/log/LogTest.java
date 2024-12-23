package org.zhuanyi.jraftdb.engine.log;

import org.testng.annotations.BeforeMethod;

import java.io.File;

import static org.testng.FileAssert.fail;

public class LogTest {

    private static final LogMonitor NO_CORRUPTION_MONITOR = new LogMonitor() {
        @Override
        public void corruption(long bytes, String reason) {
            fail(String.format("corruption of %s bytes: %s", bytes, reason));
        }

        @Override
        public void corruption(long bytes, Throwable reason) {
            throw new RuntimeException(String.format("corruption of %s bytes: %s", bytes, reason), reason);
        }
    };

    private LogRecordWriter writer;

    @BeforeMethod
    public void setUp()
            throws Exception {
        writer = LogFactory.createLogWriter(File.createTempFile("table", ".log"), 42);
    }
}
