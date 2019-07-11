package org.menina.raft.wal;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.menina.raft.common.Constants;
import org.menina.raft.common.RaftConfig;
import org.menina.raft.common.RaftUtils;
import org.menina.raft.common.meta.NextOffsetMetaData;
import org.menina.raft.message.RaftProto;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;

/**
 * @author zhenghao
 * @date 2019/2/25
 */
@Slf4j
@NotThreadSafe
public class Wal {

    public enum State {
        /**
         * already start, but unavailable
         */
        UNAVAILABLE,

        /**
         * recovering
         */
        RECOVERING,

        /**
         * recover success, available
         */
        AVAILABLE
    }

    private volatile Segment activeSegment;
    private RaftConfig config;
    private State state = State.UNAVAILABLE;
    private NavigableMap<Long, Segment> segments = new TreeMap<>();
    private ReadWriteLock lock = new ReentrantReadWriteLock();
    private Lock readLock = lock.readLock();
    private Lock writeLock = lock.writeLock();
    private long firstIndex = Constants.DEFAULT_INIT_OFFSET;

    public Wal(RaftConfig config) {
        Preconditions.checkNotNull(config);
        this.config = config;
    }

    public NextOffsetMetaData recoverWal() {
        try {
            writeLock.lock();
            state = State.RECOVERING;
            File walDir = new File(config.getWal());
            if (!walDir.exists()) {
                if (walDir.mkdirs()) {
                    log.info("create data directory {} success", walDir.getAbsolutePath());
                } else {
                    throw new IllegalStateException("failed to create directory " + walDir.getAbsolutePath());
                }
            } else {
                File[] files = walDir.listFiles();
                if (files != null) {
                    for (File file : files) {
                        if (RaftUtils.isIndexFile(file) && !RaftUtils.logFile(config.getWal(), RaftUtils.extractBaseOffset(file)).exists()) {
                            log.warn("index file {} exist but lost log file, will delete index file for consistent", file);
                            if (!file.delete()) {
                                throw new IllegalStateException(String.format("delete index file %s failed", file));
                            } else {
                                log.info("delete index file {} success", file);
                            }
                        }

                        if (RaftUtils.isLogFile(file)) {
                            File index = RaftUtils.indexFile(config.getWal(), RaftUtils.extractBaseOffset(file));
                            if (!index.exists()) {
                                log.warn("lost index file {}, recover segment for redo index", index, file);
                                recoverSegment(file);
                            } else {
                                loadSegment(file);
                            }
                        }
                    }
                } else {
                    throw new IllegalStateException("invalid file path for '\u0000', please rename file for recover success");
                }
            }

            this.loadState();
            state = State.AVAILABLE;
            activeSegment = segments.isEmpty() ? null : segments.lastEntry().getValue();
            NextOffsetMetaData nextOffsetMetaData = activeSegment == null ? new NextOffsetMetaData(0) : new NextOffsetMetaData(activeSegment.lastIndex());
            log.info("WAL start success, last offset for node {} is {}", config.getId(), nextOffsetMetaData.getOffset());
            return nextOffsetMetaData;
        } catch (IOException e) {
            throw new IllegalStateException(e.getMessage(), e);
        } finally {
            writeLock.unlock();
        }
    }

    public long startIndex() {
        return segments.size() == 0 ? 0 : segments.firstEntry().getKey();
    }

    public void write(List<RaftProto.Entry> entries) {
        Preconditions.checkNotNull(entries);
        try {
            writeLock.lock();
            entries.iterator().forEachRemaining(new Consumer<RaftProto.Entry>() {
                @Override
                public void accept(RaftProto.Entry entry) {
                    try {
                        Segment segment = maybeRoll(entry);
                        segment.append(entry);
                    } catch (IOException e) {
                        throw new IllegalStateException(e.getMessage(), e);
                    }
                }
            });
        } finally {
            writeLock.unlock();
        }
    }

    public List<RaftProto.Entry> fetch(long offset, int limit) {
        try {
            // reduce GC overhead
            writeLock.lock();
            Map.Entry<Long, Segment> segmentEntry = segments.floorEntry(offset);
            if (segmentEntry == null) {
                return Lists.newArrayList();
            }

            try {
                return segmentEntry.getValue().fetch(offset, limit);
            } catch (IOException e) {
                throw new IllegalStateException(e.getMessage(), e);
            }
        } finally {
            writeLock.unlock();
        }
    }

    public long lastIndex() {
        return activeSegment == null ? Constants.DEFAULT_INIT_OFFSET : activeSegment.lastIndex();
    }

    public RaftProto.Entry lastEntry() throws IOException {
        return activeSegment == null ? null : activeSegment.lastEntry();
    }

    public void flush() throws IOException {
        if (activeSegment != null) {
            activeSegment.flush(true);
        }
    }

    public State state() {
        return state;
    }

    public boolean truncate(long startOffset) throws IOException {
        try {
            writeLock.lock();
            if (startOffset <= lastIndex()) {
                Map.Entry<Long, Segment> startSegmentEntry = segments.floorEntry(startOffset);
                startSegmentEntry.getValue().truncate(startOffset);
                Map.Entry<Long, Segment> segmentEntry = segments.higherEntry(startSegmentEntry.getKey());
                while (segmentEntry != null) {
                    log.info("ready to delete segment for offset {}", segmentEntry.getKey());
                    segmentEntry.getValue().close();
                    segmentEntry.getValue().delete();
                    segments.remove(segmentEntry.getKey());
                    log.info("delete segment for offset {} success", segmentEntry.getKey());
                    segmentEntry = segments.higherEntry(segmentEntry.getKey());
                }
            }

            return true;
        } finally {
            writeLock.unlock();
        }
    }

    public int purge(long endOffset) throws IOException {
        try {
            writeLock.lock();
            if (segments.isEmpty() || segments.firstKey() > endOffset) {
                return 0;
            }

            Long expired = segments.floorKey(endOffset);
            int purged = 0;
            if (expired == null) {
                return purged;
            }

            Map.Entry<Long, Segment> entry = segments.lowerEntry(expired);
            while (entry != null) {
                Segment segment = entry.getValue();
                Long baseOffset = entry.getKey();
                Preconditions.checkArgument(segment != activeSegment);
                segment.close();
                segment.delete();
                segments.remove(baseOffset);
                purged++;
                log.info("purge segment for offset {} success", baseOffset);
                expired = baseOffset;
                entry = segments.lowerEntry(expired);
            }

            return purged;
        } finally {
            writeLock.unlock();
        }
    }

    public void setFirstIndex(long firstIndex) {
        this.firstIndex = firstIndex;
    }

    private Segment maybeRoll(RaftProto.Entry entry) throws IOException {
        if (activeSegment == null || entry.getIndex() == firstIndex + 1 || activeSegment.shouldRoll(entry.getData().size())) {
            long current = System.currentTimeMillis();
            activeSegment = new LogSegment(
                    this.config.getWal(),
                    entry.getIndex(),
                    this.config.getMaxSegmentTime(),
                    this.config.getMaxSegmentSize(),
                    this.config.getMaxMessageSize());
            addSegment(entry.getIndex(), activeSegment);
            log.info("roll new segment for offset {} cost {} ms", entry.getIndex(), System.currentTimeMillis() - current);
        }

        return activeSegment;
    }

    private void loadSegment(File file) throws IOException {
        long baseOffset = RaftUtils.extractBaseOffset(file);
        Segment segment = new LogSegment(
                this.config.getWal(),
                baseOffset,
                this.config.getMaxSegmentTime(),
                this.config.getMaxSegmentSize(),
                this.config.getMaxMessageSize());
        try {
            segment.safetyCheck();
        } catch (IllegalStateException e) {
            log.warn(e.getMessage());
            segment.recover();
        } finally {
            addSegment(baseOffset, segment);
        }
    }

    private void recoverSegment(File file) throws IOException {
        long baseOffset = RaftUtils.extractBaseOffset(file);
        Segment segment = new LogSegment(
                this.config.getWal(),
                baseOffset,
                this.config.getMaxSegmentTime(),
                this.config.getMaxSegmentSize(),
                this.config.getMaxMessageSize());
        segment.recover();
        addSegment(baseOffset, segment);
    }

    private void addSegment(long startOffset, Segment segment) {
        Preconditions.checkArgument(startOffset >= 0);
        Preconditions.checkNotNull(segment);
        this.segments.put(startOffset, segment);
    }

    private void loadState() throws IOException {
        for (Segment segment : segments.values()) {
            segment.loadState();
        }
    }
}
