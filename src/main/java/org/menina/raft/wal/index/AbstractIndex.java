package org.menina.raft.wal.index;

import com.google.common.base.Preconditions;
import org.menina.raft.common.Constants;
import org.menina.raft.common.RaftUtils;
import org.menina.raft.common.mmap.MappedByteBuffers;
import org.menina.raft.wal.Release;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;

/**
 * @author zhenghao
 * @date 2019/3/6
 */
@Slf4j
public abstract class AbstractIndex implements Release {

    /**
     * search range
     */
    enum Range {
        /**
         * returns element in this set equal
         */
        EQUAL,

        /**
         * returns the greatest element in this set less than or equal
         */
        FLOOR,

        /**
         * returns the least element in this set greater than or equal
         */
        CEILING
    }

    protected long baseOffset;
    protected long maxFileSize = Constants.SEGMENT_INDEX_SIZE;
    protected MappedByteBuffer mappedByteBuffer;
    protected RandomAccessFile randomAccessFile;
    protected boolean readOnly = false;
    protected int entries;
    protected int maxEntries;
    protected File file;
    protected boolean dirty;

    public AbstractIndex(String baseDir, long baseOffset, long maxFileSize, boolean readOnly) throws IOException {
        Preconditions.checkNotNull(baseDir);
        Preconditions.checkArgument(baseOffset >= 0);
        Preconditions.checkArgument(maxFileSize > 0);
        file = RaftUtils.indexFile(baseDir, baseOffset);
        boolean created = !file.exists() && file.createNewFile();
        this.randomAccessFile = new RandomAccessFile(file, this.readOnly ? "r" : "rw");
        if (created) {
            log.info("create new index file from offset {} success", baseOffset);
            randomAccessFile.setLength(maxFileSize);
        }

        this.mappedByteBuffer = randomAccessFile.getChannel().map(this.readOnly ? FileChannel.MapMode.READ_ONLY : FileChannel.MapMode.READ_WRITE, 0, randomAccessFile.length());
        if (!created) {
            this.mappedByteBuffer.position(shaping(mappedByteBuffer.limit()));
        }

        this.baseOffset = baseOffset;
        this.maxFileSize = maxFileSize;
        this.readOnly = readOnly;
        this.entries = mappedByteBuffer.position() / entrySize();
        this.maxEntries = mappedByteBuffer.limit() / entrySize();
    }

    /**
     * get data file start position by given offset
     *
     * @param offset
     * @return
     */
    public OffsetPosition translateOffset(long offset) {
        return indexRangeFor(offset, Range.FLOOR);
    }

    /**
     * flush to disk
     */
    public void force() {
        if (this.dirty) {
            this.mappedByteBuffer.force();
            this.dirty = false;
        }
    }

    /**
     * get current index entry size
     *
     * @return
     */
    public int entries() {
        return entries;
    }

    /**
     * reset index file
     */
    public void reset() {
        mappedByteBuffer.rewind();
        entries = 0;
    }

    /**
     * check index file is full
     *
     * @return
     */
    public boolean isFull() {
        return entries >= maxEntries;
    }

    /**
     * truncate index entry to given offset
     *
     * @param offset
     */
    public void truncate(long offset) {
        Integer entry = indexRangeForEntry(offset, Range.EQUAL);
        if (entry != null && entry != -1) {
            int position = entry * entrySize();
            log.info("truncate index file to position {}", position);
            this.mappedByteBuffer.position(position);
        }
    }

    /**
     * get last index entry
     *
     * @return
     */
    public OffsetPosition lastEntry() {
        return entries == 0 ? new OffsetPosition(0, 0) : parseEntry(entries - 1);
    }

    /**
     * offset relative offset for index file
     *
     * @param offset
     * @return
     */
    int relativeOffset(long offset) {
        Preconditions.checkNotNull(offset >= baseOffset);
        return (int) (offset - baseOffset);
    }

    /**
     * parse index entry
     *
     * @param n
     * @return
     */
    OffsetPosition parseEntry(int n) {
        Preconditions.checkArgument(n <= maxEntries);
        return new OffsetPosition(absoluteOffset(n), physicalPosition(n));
    }

    /**
     * get absolute offset
     *
     * @param n
     * @return
     */
    private long absoluteOffset(int n) {
        Preconditions.checkArgument(n >= 0);
        return baseOffset + mappedByteBuffer.getInt(n * entrySize());
    }

    /**
     * get position for data file by given index entry
     *
     * @param n
     * @return
     */
    private int physicalPosition(int n) {
        return mappedByteBuffer.getInt(n * entrySize() + Integer.BYTES);
    }

    /**
     * partial write process
     *
     * @param offset
     * @return
     */
    private int shaping(int offset) {
        return entrySize() * (offset / entrySize());
    }

    /**
     * get data file start position by given offset
     *
     * @param offset
     * @param range
     * @return
     */
    private OffsetPosition indexRangeFor(long offset, Range range) {
        Preconditions.checkNotNull(range);
        Integer entry = indexRangeForEntry(offset, range);
        if (entry == null) {
            return null;
        }

        return entry == -1 ? new OffsetPosition(0, 0) : parseEntry(entry);
    }

    /**
     * binary search for index entry
     *
     * @param offset
     * @param range
     * @return
     */
    private Integer indexRangeForEntry(long offset, Range range) {
        Preconditions.checkNotNull(range);
        if (entries == 0 || parseEntry(0).getOffset() > offset) {
            return -1;
        }

        int lo = 0;
        int high = entries();
        while (lo < high) {
            int mid = lo + ((high - lo) >> 1);
            OffsetPosition offsetPosition = parseEntry(mid);
            if (offsetPosition.getOffset() < offset) {
                lo = mid + 1;
            } else if (offsetPosition.getOffset() > offset) {
                high = mid - 1;
            } else {
                return mid;
            }
        }

        // guaranteed floor semantics
        if (lo == high) {
            OffsetPosition offsetPosition = parseEntry(lo);
            while (offsetPosition.getOffset() > offset) {
                Preconditions.checkNotNull(lo >= 0);
                lo = lo - 1;
                offsetPosition = parseEntry(lo);
            }
        }

        // guaranteed floor semantics
        if (lo > high) {
            lo = high;
        }

        switch (range) {
            case EQUAL:
                return null;
            case FLOOR:
                return lo;
            case CEILING:
                return lo == mappedByteBuffer.limit() / entrySize() - 1 ? null : lo + 1;
            default:
                throw new IllegalStateException();
        }
    }

    /**
     * close index file
     *
     * @throws IOException
     */
    @Override
    public void close() throws IOException {
        try {
            this.randomAccessFile.close();
            MappedByteBuffers.unmap(file.getAbsolutePath(), mappedByteBuffer);
        } catch (Throwable t) {
            log.error(t.getMessage(), t);
        } finally {
            mappedByteBuffer = null;
        }
    }

    /**
     * delete index file
     */
    @Override
    public void delete() throws IOException {
        Preconditions.checkNotNull(this.file);
        if (!Files.deleteIfExists(this.file.toPath())) {
            throw new IllegalStateException(String.format("delete %s failed", this.file));
        }
    }

    /**
     * append index entry
     *
     * @param offset
     * @param position
     */
    public abstract void append(long offset, int position);

    /**
     * get entry size
     *
     * @return
     */
    public abstract int entrySize();

    /**
     * check index file is broken
     */
    public abstract void safetyCheck();

}
