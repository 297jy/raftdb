package org.zhuanyi.jraftdb.engine.utils.file;

import org.zhuanyi.common.Closeables;
import org.zhuanyi.common.LogUtils;
import org.zhuanyi.jraftdb.engine.utils.ByteBufferSupport;
import org.zhuanyi.jraftdb.engine.utils.slice.Slice;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;


public class MMapWritableFile extends BaseWritableFile {

    private static final int PAGE_SIZE = 1024 * 1024;

    private final FileChannel fileChannel;

    private MappedByteBuffer mappedByteBuffer;

    private long fileOffset;

    public MMapWritableFile(File file, long fileNumber) throws IOException {
        super(file, fileNumber);
        this.fileChannel = new RandomAccessFile(file, "rw").getChannel();
        this.mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, PAGE_SIZE);
    }

    @Override
    protected int doWrite(byte[] data, int offset, int size) {

        int writeSize = Math.min(size, mappedByteBuffer.remaining());
        mappedByteBuffer.put(data, offset, writeSize);
        return writeSize;
    }

    @Override
    protected int doWrite(Slice data, int offset, int size) {
        return data.writeBytesToBuffer(offset, mappedByteBuffer, size);
    }

    @Override
    protected boolean doSync(boolean metaData) {
        try {
            fileChannel.force(metaData);
        } catch (IOException ex) {
            LogUtils.info(String.format("MMapWritableFile-doSync-ex:{%s}", ex.getMessage()));
            return false;
        }
        return true;
    }

    @Override
    protected boolean doClose() {
        destroyMappedByteBuffer();
        try {
            if (fileChannel.isOpen()) {
                fileChannel.truncate(fileOffset);
            }
        } catch (IOException ex) {
            LogUtils.info(String.format("MMapWritableFile-doClose-ex:{%s}", ex.getMessage()));
        }

        // close the channel
        Closeables.closeQuietly(fileChannel);
        return false;
    }

    private void destroyMappedByteBuffer() {
        if (mappedByteBuffer != null) {
            fileOffset += mappedByteBuffer.position();
            unmap();
        }
        mappedByteBuffer = null;
    }

    private void unmap() {
        ByteBufferSupport.unmap(mappedByteBuffer);
    }
}
