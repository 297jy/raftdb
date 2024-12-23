package org.zhuanyi.jraftdb.engine.utils.file;

import org.zhuanyi.common.Closeables;
import org.zhuanyi.common.LogUtils;
import org.zhuanyi.jraftdb.engine.utils.slice.Slice;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class FileChannelWritableFile extends BaseWritableFile {


    private final FileChannel fileChannel;

    public FileChannelWritableFile(File file, long fileNumber) throws FileNotFoundException {
        super(file, fileNumber);
        this.fileChannel = new FileOutputStream(file).getChannel();
    }

    @Override
    public boolean doClose() {
        // try to forces the log to disk
        try {
            fileChannel.force(true);
        } catch (IOException ex) {
            LogUtils.info(String.format("FileChannelWritableFile-doClose-ex:{%s}", ex.getMessage()));
            return false;
        }

        // close the channel
        Closeables.closeQuietly(fileChannel);
        return true;
    }

    @Override
    protected int doWrite(byte[] data, int offset, int size) {
        try {
            return fileChannel.write(ByteBuffer.wrap(data, offset, size));
        } catch (IOException ex) {
            LogUtils.info(String.format("FileChannelWritableFile-doWrite-ex:{%s}", ex.getMessage()));
            return -1;
        }
    }

    @Override
    protected int doWrite(Slice data, int offset, int size) {
        try {
            return data.writeBytesToChannel(offset, fileChannel, size);
        } catch (IOException ex) {
            LogUtils.info(String.format("FileChannelWritableFile-doWrite-ex:{%s}", ex.getMessage()));
            return -1;
        }
    }

    @Override
    protected boolean doSync(boolean metaData) {
        try {
            fileChannel.force(metaData);
        } catch (IOException ex) {
            LogUtils.info(String.format("FileChannelWritableFile-doSync-ex:{%s}", ex.getMessage()));
            return false;
        }
        return true;
    }
}
