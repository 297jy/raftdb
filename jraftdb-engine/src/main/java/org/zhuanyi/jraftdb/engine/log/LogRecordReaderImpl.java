package org.zhuanyi.jraftdb.engine.log;

import org.zhuanyi.jraftdb.engine.utils.DynamicSliceOutput;
import org.zhuanyi.jraftdb.engine.utils.slice.Slice;
import org.zhuanyi.jraftdb.engine.utils.slice.SliceInput;
import org.zhuanyi.jraftdb.engine.utils.slice.SliceOutput;
import org.zhuanyi.jraftdb.engine.utils.slice.Slices;

import java.io.IOException;
import java.nio.channels.FileChannel;

import static org.zhuanyi.jraftdb.engine.constant.LogConstants.BLOCK_SIZE;
import static org.zhuanyi.jraftdb.engine.constant.LogConstants.HEADER_SIZE;
import static org.zhuanyi.jraftdb.engine.log.LogChunkType.*;
import static org.zhuanyi.jraftdb.engine.log.Logs.getChunkChecksum;

public class LogRecordReaderImpl implements LogRecordReader {

    private final FileChannel fileChannel;

    private final LogMonitor monitor;

    private final boolean verifyChecksums;

    /**
     * Have we read to the end of the file?
     */
    private boolean eof;

    /**
     * Offset of the last record returned by readRecord.
     */
    private long lastRecordOffset;

    /**
     * Offset of the first location past the end of buffer.
     */
    private long endOfBufferOffset;

    /**
     * Offset at which to start looking for the first record to return
     */
    private final long initialOffset;

    /**
     * Scratch buffer in which the next record is assembled.
     */
    private final DynamicSliceOutput recordScratch = new DynamicSliceOutput(BLOCK_SIZE);

    /**
     * Scratch buffer for current block.  The currentBlock is sliced off the underlying buffer.
     */
    private final SliceOutput blockScratch = Slices.allocate(BLOCK_SIZE).output();

    /**
     * The current block records are being read from.
     */
    private SliceInput currentBlock = Slices.EMPTY_SLICE.input();

    /**
     * Current chunk which is sliced from the current block.
     */
    private Slice currentChunk = Slices.EMPTY_SLICE;

    public LogRecordReaderImpl(FileChannel fileChannel, LogMonitor monitor, boolean verifyChecksums, long initialOffset) {
        this.fileChannel = fileChannel;
        this.monitor = monitor;
        this.verifyChecksums = verifyChecksums;
        this.initialOffset = initialOffset;
    }

    @Override
    public Slice readRecord() {

        recordScratch.reset();
        // 当前偏移 < 指定的偏移，需要seek到指定位置
        if (lastRecordOffset < initialOffset) {
            if (!skipToInitialBlock()) {
                return null;
            }
        }

        // Record offset of the logical record that we're reading
        long prospectiveRecordOffset = 0;

        boolean inFragmentedRecord = false;
        while (true) {
            long physicalRecordOffset = endOfBufferOffset - currentChunk.length();
            LogChunkType chunkType = readNextChunk();
            switch (chunkType) {
                case FULL:
                    if (inFragmentedRecord) {
                        reportCorruption(recordScratch.size(), "Partial record without end");
                        // simply return this full block
                    }
                    recordScratch.reset();
                    prospectiveRecordOffset = physicalRecordOffset;
                    lastRecordOffset = prospectiveRecordOffset;
                    return currentChunk.copySlice();

                case FIRST:
                    if (inFragmentedRecord) {
                        reportCorruption(recordScratch.size(), "Partial record without end");
                        // clear the scratch and start over from this chunk
                        recordScratch.reset();
                    }
                    prospectiveRecordOffset = physicalRecordOffset;
                    recordScratch.writeBytes(currentChunk);
                    inFragmentedRecord = true;
                    break;
                case MIDDLE:
                    if (!inFragmentedRecord) {
                        reportCorruption(recordScratch.size(), "Missing start of fragmented record");

                        // clear the scratch and skip this chunk
                        recordScratch.reset();
                    } else {
                        recordScratch.writeBytes(currentChunk);
                    }
                    break;
                case LAST:
                    if (!inFragmentedRecord) {
                        reportCorruption(recordScratch.size(), "Missing start of fragmented record");

                        // clear the scratch and skip this chunk
                        recordScratch.reset();
                    } else {
                        recordScratch.writeBytes(currentChunk);
                        lastRecordOffset = prospectiveRecordOffset;
                        return recordScratch.slice().copySlice();
                    }
                    break;
                case EOF:
                    if (inFragmentedRecord) {
                        reportCorruption(recordScratch.size(), "Partial record without end");

                        // clear the scratch and return
                        recordScratch.reset();
                    }
                    return null;
                case BAD_CHUNK:
                    if (inFragmentedRecord) {
                        reportCorruption(recordScratch.size(), "Error in middle of record");
                        inFragmentedRecord = false;
                        recordScratch.reset();
                    }
                    break;
                default:
                    int dropSize = currentChunk.length();
                    if (inFragmentedRecord) {
                        dropSize += recordScratch.size();
                    }
                    reportCorruption(dropSize, String.format("Unexpected chunk type %s", chunkType));
                    inFragmentedRecord = false;
                    recordScratch.reset();
                    break;
            }
        }
    }

    /**
     * Return type, or one of the preceding special values
     */
    private LogChunkType readNextChunk() {
        // clear the current chunk
        currentChunk = Slices.EMPTY_SLICE;
        // 读取一个完整的block
        if (currentBlock.available() < HEADER_SIZE) {
            if (!readNextBlock()) {
                if (eof) {
                    return EOF;
                }
            }
        }

        // parse header
        int expectedChecksum = currentBlock.readInt();
        int length = currentBlock.readUnsignedByte();
        length = length | currentBlock.readUnsignedByte() << 8;
        byte chunkTypeId = currentBlock.readByte();
        LogChunkType chunkType = getLogChunkTypeByPersistentId(chunkTypeId);
        // verify length
        if (length > currentBlock.available()) {
            int dropSize = currentBlock.available() + HEADER_SIZE;
            reportCorruption(dropSize, "Invalid chunk length");
            currentBlock = Slices.EMPTY_SLICE.input();
            return BAD_CHUNK;
        }

        if (chunkType == ZERO_TYPE && length == 0) {
            currentBlock = Slices.EMPTY_SLICE.input();
            return BAD_CHUNK;
        }

        // Skip physical record that started before initialOffset
        if (endOfBufferOffset - HEADER_SIZE - length < initialOffset) {
            currentBlock.skipBytes(length);
            return BAD_CHUNK;
        }

        // read the chunk
        currentChunk = currentBlock.readBytes(length);
        if (verifyChecksums) {
            int actualChecksum = getChunkChecksum(chunkTypeId, currentChunk);
            if (actualChecksum != expectedChecksum) {
                // Drop the rest of the buffer since "length" itself may have
                // been corrupted and if we trust it, we could find some
                // fragment of a real log record that just happens to look
                // like a valid log record.
                int dropSize = currentBlock.available() + HEADER_SIZE;
                currentBlock = Slices.EMPTY_SLICE.input();
                reportCorruption(dropSize, "Invalid chunk checksum");
                return BAD_CHUNK;
            }
        }
        // Skip unknown chunk types
        // Since this comes last so we the, know it is a valid chunk, and is just a type we don't understand
        if (chunkType == UNKNOWN) {
            reportCorruption(length, String.format("Unknown chunk type %d", chunkType.getPersistentId()));
            return BAD_CHUNK;
        }

        return chunkType;
    }

    public boolean readNextBlock() {
        if (eof) {
            return false;
        }

        // clear the block
        blockScratch.reset();

        // read the next full block
        while (blockScratch.writableBytes() > 0) {
            try {
                int bytesRead = blockScratch.writeBytes(fileChannel, blockScratch.writableBytes());
                if (bytesRead < 0) {
                    // no more bytes to read
                    eof = true;
                    break;
                }
                endOfBufferOffset += bytesRead;
            } catch (IOException e) {
                currentBlock = Slices.EMPTY_SLICE.input();
                reportDrop(BLOCK_SIZE, e);
                eof = true;
                return false;
            }

        }
        currentBlock = blockScratch.slice().input();
        return currentBlock.isReadable();
    }

    /**
     * Skips all blocks that are completely before "initial_offset_".
     * <p/>
     * Handles reporting corruption
     *
     * @return true on success.
     */
    private boolean skipToInitialBlock() {
        // 计算在block内的偏移位置，并调整到开始读取block的起始位置
        int offsetInBlock = (int) (initialOffset % BLOCK_SIZE);
        long blockStartLocation = initialOffset - offsetInBlock;

        // Don't search a block if we'd be in the trailer
        if (offsetInBlock > BLOCK_SIZE - 6) {
            blockStartLocation += BLOCK_SIZE;
        }

        endOfBufferOffset = blockStartLocation;

        // Skip to start of first block that can contain the initial record
        if (blockStartLocation > 0) {
            try {
                fileChannel.position(blockStartLocation);
            } catch (IOException e) {
                reportDrop(blockStartLocation, e);
                return false;
            }
        }

        return true;
    }

    /**
     * Reports corruption to the monitor.
     * The buffer must be updated to remove the dropped bytes prior to invocation.
     */
    private void reportCorruption(long bytes, String reason) {
        if (monitor != null) {
            monitor.corruption(bytes, reason);
        }
    }

    /**
     * Reports dropped bytes to the monitor.
     * The buffer must be updated to remove the dropped bytes prior to invocation.
     */
    private void reportDrop(long bytes, Throwable reason) {
        if (monitor != null) {
            monitor.corruption(bytes, reason);
        }
    }
}
