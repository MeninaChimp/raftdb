package org.menina.raft.wal;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.menina.raft.common.Constants;
import org.menina.raft.common.RaftUtils;
import org.menina.raft.exception.IllegalSeekException;
import org.menina.raft.message.RaftProto;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

/**
 * @author zhenghao
 * @date 2019/3/13
 */
@Slf4j
@NotThreadSafe
public class FileRecords implements Records {

    private FileChannel channel;
    private long baseOffset;
    private long lastOffset = Constants.DEFAULT_INIT_OFFSET;
    private int sizeOfBytes;
    private long maxLogSize;
    private long lastModified;
    private ByteBuffer appendBuffer;
    private ByteBuffer fetchBuffer;
    private ByteBuffer header;
    private RandomAccessFile randomAccessFile;
    private File file;

    public FileRecords(String baseDir, long baseOffset, long maxLogSize) throws IOException {
        Preconditions.checkNotNull(baseDir);
        Preconditions.checkArgument(baseOffset >= 0);
        Preconditions.checkArgument(maxLogSize > 0);
        this.file = RaftUtils.logFile(baseDir, baseOffset);
        if (!file.exists() && file.createNewFile()) {
            log.info("create new log file from offset {} success", baseOffset);
        }

        this.maxLogSize = maxLogSize;
        this.randomAccessFile = new RandomAccessFile(file, "rw");
        this.channel = randomAccessFile.getChannel();
        this.baseOffset = baseOffset;
        this.sizeOfBytes = (int) this.channel.size();
        this.channel.position(sizeOfBytes);
        this.lastModified = file.lastModified();
        this.appendBuffer = ByteBuffer.allocate(HEADER_OFFSET + Constants.MAX_ENTRY_SIZE);
        this.fetchBuffer = ByteBuffer.allocate(HEADER_OFFSET + Constants.MAX_ENTRY_SIZE);
        this.header = ByteBuffer.allocate(HEADER_OFFSET);
        this.lastOffset = baseOffset - 1;
    }

    @Override
    public int append(RaftProto.Entry entry) throws IOException {
        byte[] data = entry.toByteArray();
        appendBuffer.clear();
        appendBuffer.putLong(entry.getIndex());
        appendBuffer.putLong(0);
        appendBuffer.putInt(data.length);
        appendBuffer.put(data);
        appendBuffer.flip();
        while (appendBuffer.hasRemaining()) {
            channel.write(appendBuffer);
        }

        int startPosition = sizeOfBytes;
        sizeOfBytes += appendBuffer.limit();
        lastModified = System.currentTimeMillis();
        lastOffset += 1;
        return startPosition;
    }

    @Override
    public List<RaftProto.Entry> read(int position, long offset, int maxSize) throws IOException {
        Preconditions.checkArgument(offset >= baseOffset);
        Preconditions.checkArgument(position >= 0);
        if (position + HEADER_OFFSET > sizeOfBytes) {
            return Lists.newArrayList();
        }

        header.clear();
        channel.read(header, position);
        header.flip();
        long current = header.getLong();
        while (current < offset) {
            position += HEADER_OFFSET + header.getInt(SIZE_OFFSET);
            if (position + HEADER_OFFSET > sizeOfBytes) {
                return Lists.newArrayList();
            }

            header.rewind();
            channel.read(header, position);
            header.flip();
            current = header.getLong();
        }

        if (current != offset) {
            throw new IllegalSeekException("illegal seek offset for offset " + offset + ", current seek " + current);
        }

        List<RaftProto.Entry> entries = new ArrayList<>();
        for (int i = 0; i < maxSize; i++) {
            if (position + HEADER_OFFSET > sizeOfBytes) {
                break;
            }

            header.clear();
            channel.read(header, position);
            header.flip();
            int entrySize = header.getInt(SIZE_OFFSET);
            if (position + HEADER_OFFSET + entrySize > sizeOfBytes) {
                break;
            }

            fetchBuffer.clear();
            fetchBuffer.limit(entrySize);
            channel.read(fetchBuffer, position + HEADER_OFFSET);
            fetchBuffer.flip();
            entries.add(RaftProto.Entry.parseFrom(fetchBuffer));
            position += HEADER_OFFSET + fetchBuffer.limit();
        }

        return entries;
    }

    @Override
    public int seekTo(int position, long offset) throws IOException {
        Preconditions.checkArgument(offset >= baseOffset);
        Preconditions.checkArgument(position >= 0);
        Preconditions.checkArgument(position + HEADER_OFFSET < sizeOfBytes);
        header.clear();
        channel.read(header, position);
        header.flip();
        long current = header.getLong();
        while (current < offset) {
            position += HEADER_OFFSET + header.getInt(SIZE_OFFSET);
            Preconditions.checkArgument(position + HEADER_OFFSET < sizeOfBytes);
            header.rewind();
            channel.read(header, position);
            header.flip();
            current = header.getLong();
        }

        Preconditions.checkArgument(current == offset);
        return position;
    }

    @Override
    public boolean canAppend(int appendSize) {
        return maxLogSize > HEADER_OFFSET + appendSize + sizeOfBytes;
    }

    @Override
    public void flush(boolean metaData) throws IOException {
        this.channel.force(metaData);
    }

    @Override
    public long largestTimestamp() {
        return lastModified;
    }

    @Override
    public int truncate(int position) throws IOException {
        this.channel.truncate(position);
        int lastSizeOfBytes = this.sizeOfBytes;
        this.sizeOfBytes = position;
        return lastSizeOfBytes - position;
    }

    @Override
    public long lastOffset() {
        return lastOffset;
    }

    @Override
    public void updateLastOffset(long lastOffset) {
        Preconditions.checkArgument(lastOffset >= baseOffset);
        this.lastOffset = lastOffset;
    }

    @Override
    public void close() throws IOException {
        this.randomAccessFile.close();
    }

    @Override
    public void delete() throws IOException {
        Preconditions.checkNotNull(this.file);
        if (!Files.deleteIfExists(this.file.toPath())) {
            throw new IllegalStateException(String.format("delete %s failed", this.file));
        }
    }

    @Override
    public String toString() {
        return "FileRecords{" +
                "baseOffset=" + baseOffset +
                ", sizeOfBytes=" + sizeOfBytes +
                ", lastModified=" + lastModified +
                ", file=" + file +
                '}';
    }
}
