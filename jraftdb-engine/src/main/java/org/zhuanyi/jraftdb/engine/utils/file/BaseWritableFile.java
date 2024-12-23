package org.zhuanyi.jraftdb.engine.utils.file;

import org.zhuanyi.jraftdb.engine.constant.FileMetaConstants;
import org.zhuanyi.jraftdb.engine.dto.Status;
import org.zhuanyi.jraftdb.engine.utils.FileUtils;
import org.zhuanyi.jraftdb.engine.utils.slice.Slice;

import java.io.File;
import java.util.concurrent.atomic.AtomicBoolean;


public abstract class BaseWritableFile {

    protected File file;

    protected byte[] buf;

    protected int pos;

    protected boolean isManifest;

    private final long fileNumber;

    private final AtomicBoolean closed = new AtomicBoolean();


    public BaseWritableFile(File file, long fileNumber) {
        this.file = file;
        this.buf = new byte[FileMetaConstants.WRITABLE_FILE_BUFFER_SIZE];
        this.pos = 0;
        this.isManifest = FileUtils.isManifest(file);
        this.fileNumber = fileNumber;
    }

    public long getFileNumber() {
        return fileNumber;
    }

    public boolean isClosed() {
        return closed.get();
    }

    public Status delete() {
        close();
        return file.delete() ? Status.ok() : Status.ioError("Delete failed");
    }

    /**
     * 追加数据到缓冲区
     *
     * @param data
     * @return
     */
    public Status append(Slice data) {
        int writeSize = data.length();
        int copySize = Math.min(writeSize, FileMetaConstants.WRITABLE_FILE_BUFFER_SIZE - pos);
        data.copyBytes(0, buf, pos, copySize);
        writeSize -= copySize;
        pos += copySize;
        if (writeSize == 0) {
            return Status.ok();
        }

        Status status = flushBuffer();
        if (!status.isOk()) {
            return status;
        }

        // 如果剩余待写入的数据能够放进缓冲区中，就先放到缓冲区
        if (writeSize < FileMetaConstants.WRITABLE_FILE_BUFFER_SIZE) {
            data.copyBytes(copySize, buf, pos, writeSize);
            pos += writeSize;
            return Status.ok();
        }

        // 如果放不下就直接写入文件了
        int res = doWrite(data, copySize, writeSize);
        return res > 0 ? Status.ok() : Status.ioError("doWrite error");
    }

    private Status flushBuffer() {
        Status status = writeUnbuffered();
        pos = 0;
        return status;
    }

    private Status writeUnbuffered() {
        if (pos <= 0) {
            return Status.ok();
        }
        int res = doWrite(buf, 0, pos);
        return res > 0 ? Status.ok() : Status.ioError("writeUnbuffered error");
    }

    /**
     * 刷新缓冲区
     *
     * @return
     */
    public Status flush() {
        return flushBuffer();
    }

    /**
     * 将数据同步写入文件并落盘，因为数据即使被write，也只是保存在pagecache中，存在数据丢失的风险
     *
     * @return
     */
    public Status sync(boolean metaSync) {
        Status status = flushBuffer();
        if (!status.isOk()) {
            return status;
        }

        // 如果是Manifest文件需要刷新
        return doSync(metaSync) ? Status.ok() : Status.ioError("sync error");
    }

    /**
     * 关闭并刷新缓冲区
     *
     * @return
     */
    public Status close() {
        closed.set(true);
        return doClose() ? Status.ok() : Status.ioError("doClose error");
    }

    public File getFile() {
        return file;
    }

    /**
     * 将缓冲区中的数据写入到文件中
     *
     * @param offset 需要写入数据在buf数组中的偏移量
     * @param size   需要写入数据的字节大小
     * @return
     */
    protected abstract int doWrite(byte[] data, int offset, int size);

    /**
     * 将data中的数据写入文件
     *
     * @param data
     * @param offset
     * @param size
     * @return
     */
    protected abstract int doWrite(Slice data, int offset, int size);

    /**
     * 将数据同步写入文件并落盘，因为数据即使被write，也只是保存在pagecache中，存在数据丢失的风险
     *
     * @param metaData 元数据
     * @return
     */
    protected abstract boolean doSync(boolean metaData);

    protected abstract boolean doClose();

}
