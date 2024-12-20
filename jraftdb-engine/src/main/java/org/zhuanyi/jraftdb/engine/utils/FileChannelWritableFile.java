package org.zhuanyi.jraftdb.engine.utils;

import org.zhuanyi.jraftdb.engine.dto.Status;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.nio.channels.FileChannel;

public class FileChannelWritableFile extends BaseWritableFile {


    private final FileChannel fileChannel;

    public FileChannelWritableFile(File file) throws FileNotFoundException {
        super(file);
        this.fileChannel = new FileOutputStream(file).getChannel();
    }

    @Override
    public Status close() {
        return null;
    }

    @Override
    public Status flush() {
        return null;
    }

    @Override
    public Status sync() {
        return null;
    }

    @Override
    public int doWrite(byte[] data, int offset, int size) {
        return 0;
    }

    @Override
    public int doWrite(Slice data, int offset, int size) {
        return 0;
    }
}
