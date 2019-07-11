package org.menina.raft.wal;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.menina.raft.common.Constants;
import org.menina.raft.common.RaftUtils;
import org.menina.raft.exception.IllegalSeekException;
import org.menina.raft.message.RaftProto;
import org.menina.raft.wal.index.OffsetIndex;
import org.menina.raft.wal.index.OffsetPosition;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.List;

/**
 * @author zhenghao
 * @date 2019/2/27
 */
@Slf4j
public class LogSegment implements Segment {

    private long baseOffset;
    private long maxSegmentTime;
    private FileRecords fileRecords;
    private OffsetIndex offsetIndex;

    public LogSegment(String baseDir, long baseOffset, long maxSegmentTime, long maxSegmentSize, int maxMessageSize) throws IOException {
        this.baseOffset = baseOffset;
        this.maxSegmentTime = maxSegmentTime;
        this.fileRecords = new FileRecords(baseDir, baseOffset, maxSegmentSize, maxMessageSize);
        this.offsetIndex = new OffsetIndex(baseDir, baseOffset, Constants.SEGMENT_INDEX_SIZE, true, true);
    }

    @Override
    public void append(RaftProto.Entry entry) throws IOException {
        Preconditions.checkNotNull(entry);
        int position = fileRecords.append(entry);
        offsetIndex.append(entry.getIndex(), position);
    }

    @Override
    public void append(List<RaftProto.Entry> entries) throws IOException {
        Preconditions.checkNotNull(entries);
        for (RaftProto.Entry entry : entries) {
            int position = fileRecords.append(entry);
            offsetIndex.append(entry.getIndex(), position);
        }
    }

    @Override
    public List<RaftProto.Entry> fetch(long offset, int limit) throws IOException {
        if (limit == 0) {
            return Lists.newArrayList();
        }

        Preconditions.checkArgument(offset >= baseOffset);
        Preconditions.checkArgument(limit > 0);
        OffsetPosition offsetPosition = offsetIndex.translateOffset(offset);
        Preconditions.checkArgument(offsetPosition.getOffset() <= offset);
        return fileRecords.read(offsetPosition.getPosition(), offset, limit);
    }

    @Override
    public boolean shouldRoll(int appendSize) {
        return offsetIndex.isFull()
                || !fileRecords.canAppend(appendSize)
                || System.currentTimeMillis() - fileRecords.largestTimestamp() > maxSegmentTime;
    }

    @Override
    public void recover() throws IOException {
        log.info("start recover segment from offset {}", baseOffset);
        offsetIndex.reset();
        List<RaftProto.Entry> entries;
        int position = 0;
        long offset = baseOffset;
        long sequentialIndexTrack = baseOffset;
        long recoverStartTimestamp = System.currentTimeMillis();
        do {
            try {
                entries = fileRecords.read(position, offset, Constants.DEFAULT_RECOVER_BATCH_SIZE);
                for (RaftProto.Entry entry : entries) {
                    if (!RaftUtils.checkCrc(entry.getData().toByteArray(), entry.getCrc())) {
                        log.warn("detect dirty data for entry {}, truncate log from index {}, will interrupt recover current segment", this, entry.getIndex());
                        truncate(entry.getIndex());
                        break;
                    }

                    if (entry.getIndex() != sequentialIndexTrack) {
                        throw new IllegalStateException("detect non-sequential offset, current offset " + entry.getIndex() + " , need sequentialIndexTrack " + sequentialIndexTrack);
                    }

                    sequentialIndexTrack++;
                    offsetIndex.append(entry.getIndex(), position);
                    offset += 1;
                    position += entry.getSerializedSize() + Records.HEADER_OFFSET;
                }
            } catch (IllegalSeekException e) {
                log.error(e.getMessage());
                log.info("detect partial write for data file in segment[{}], truncate log from broken index {}(included)", baseOffset, offset);
                fileRecords.truncate(position);
                break;
            }
        } while (entries.size() > 0);

        log.info("recover segment for offset {} success, time cost {} ms", baseOffset, System.currentTimeMillis() - recoverStartTimestamp);
    }

    @Override
    public void safetyCheck() {
        offsetIndex.safetyCheck();
    }

    @Override
    public long lastIndex() {
        return fileRecords.lastOffset();
    }

    @Override
    public RaftProto.Entry lastEntry() throws IOException {
        List<RaftProto.Entry> last = fetch(lastIndex(), 1);
        return last.size() > 0 ? last.get(0) : null;
    }

    @Override
    public void loadState() throws IOException {
        OffsetPosition offsetPosition = offsetIndex.lastEntry();
        // the situation of entries.size() > Constants.DEFAULT_RECOVER_BATCH_SIZE will never occur
        List<RaftProto.Entry> entries = fileRecords.read(offsetPosition.getPosition(), offsetPosition.getOffset(), Constants.DEFAULT_RECOVER_BATCH_SIZE);
        if (entries.size() == 0) {
            fileRecords.updateLastOffset(offsetPosition.getOffset());
        } else {
            fileRecords.updateLastOffset(entries.get(entries.size() - 1).getIndex());
        }
    }

    @Override
    public void truncate(long offset) throws IOException {
        OffsetPosition offsetPosition = offsetIndex.translateOffset(offset);
        if (offsetPosition == null) {
            log.warn("illegal offset {} to truncate, log and index will not modify");
            return;
        }

        int truncatedBytes = fileRecords.truncate(fileRecords.seekTo(offsetPosition.getPosition(), offset));
        log.info("truncate success for segment {}, truncate bytes {}", this, truncatedBytes);
        offsetIndex.truncate(offset);
    }

    @Override
    public void flush(boolean metaData) throws IOException {
        fileRecords.flush(metaData);
        offsetIndex.force();
    }

    @Override
    public void close() throws IOException {
        fileRecords.close();
        offsetIndex.close();
    }

    @Override
    public void delete() throws IOException {
        fileRecords.delete();
        offsetIndex.delete();
    }

    @Override
    public String toString() {
        return "LogSegment{" +
                "baseOffset=" + baseOffset +
                ", maxSegmentTime=" + maxSegmentTime +
                '}';
    }
}
