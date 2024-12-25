package org.zhuanyi.jraftdb.engine.log;

import org.zhuanyi.jraftdb.engine.utils.slice.Slice;
import org.zhuanyi.jraftdb.engine.utils.slice.SliceInput;
import org.zhuanyi.jraftdb.engine.utils.file.BaseWritableFile;
import org.zhuanyi.jraftdb.engine.utils.slice.SliceOutput;
import org.zhuanyi.jraftdb.engine.utils.slice.Slices;

import java.nio.ByteBuffer;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static org.zhuanyi.jraftdb.engine.constant.LogConstants.BLOCK_SIZE;
import static org.zhuanyi.jraftdb.engine.constant.LogConstants.HEADER_SIZE;

/**
 * 进行预写日志的写
 */
public class LogRecordWriterImpl implements LogRecordWriter {

    private final BaseWritableFile dest;

    private int blockOffset;

    private static final Slice[] BLOCK_PADDING_ARR = new Slice[7];

    static {
        for (int i = 0; i < BLOCK_PADDING_ARR.length; i++) {
            BLOCK_PADDING_ARR[i] = new Slice(i);
        }
    }

    public LogRecordWriterImpl(BaseWritableFile dest) {
        this.dest = dest;
        this.blockOffset = 0;
    }

    @Override
    public BaseWritableFile getWritableFile() {
        return dest;
    }

    @Override
    public void addRecord(Slice record) {
        checkState(!dest.isClosed(), "Log has been closed");
        SliceInput sliceInput = record.input();
        boolean begin = true;
        do {
            int bytesRemainingInBlock = BLOCK_SIZE - blockOffset;
            checkState(bytesRemainingInBlock >= 0);
            // 如果剩余的空间连block的header部分都放不下，那需要用0把当前的block剩余空间填完，因为每个block都是32KB对齐的
            if (bytesRemainingInBlock < HEADER_SIZE) {
                if (bytesRemainingInBlock > 0) {
                    dest.append(BLOCK_PADDING_ARR[bytesRemainingInBlock]);
                    //dest.append(new Slice(ByteBuffer.allocate(bytesRemainingInBlock)));
                }

            }
            blockOffset = 0;
            bytesRemainingInBlock = BLOCK_SIZE;

            // 计算当前block中有多少空间可以存放log的data部分
            int bytesAvailableInBlock = bytesRemainingInBlock - HEADER_SIZE;
            checkState(bytesAvailableInBlock >= 0);

            boolean end;
            int fragmentLength;
            if (sliceInput.available() > bytesAvailableInBlock) {
                end = false;
                fragmentLength = bytesAvailableInBlock;
            } else {
                end = true;
                fragmentLength = sliceInput.available();
            }

            LogChunkType type;
            if (begin && end) {
                type = LogChunkType.FULL;
            } else if (begin) {
                type = LogChunkType.FIRST;
            } else if (end) {
                type = LogChunkType.LAST;
            } else {
                type = LogChunkType.MIDDLE;
            }

            writeChunk(type, sliceInput.readSlice(fragmentLength));

            begin = false;
        } while (sliceInput.isReadable());
    }

    private void writeChunk(LogChunkType type, Slice slice) {
        checkArgument(slice.length() <= 0xffff, "length %s is larger than two bytes", slice.length());
        checkArgument(blockOffset + HEADER_SIZE <= BLOCK_SIZE);

        // create header
        Slice header = newLogRecordHeader(type, slice, slice.length());

        // write the header and the payload
        dest.append(header);
        dest.append(slice);
        blockOffset += HEADER_SIZE + slice.length();
    }

    private Slice newLogRecordHeader(LogChunkType type, Slice slice, int length) {
        int crc = Logs.getChunkChecksum(type.getPersistentId(), slice.getRawArray(), slice.getRawOffset(), length);

        // Format the header
        SliceOutput header = Slices.allocate(HEADER_SIZE).output();
        header.writeInt(crc);
        header.writeByte((byte) (length & 0xff));
        header.writeByte((byte) (length >>> 8));
        header.writeByte((byte) (type.getPersistentId()));

        return header.slice();
    }
}
