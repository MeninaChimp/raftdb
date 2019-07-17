package org.menina.raft.storage;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.menina.raft.common.Constants;
import org.menina.raft.common.RingBuffer;
import org.menina.raft.message.RaftProto;
import org.menina.raft.wal.Wal;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author zhenghao
 * @date 2019/4/17
 */
@Slf4j
public class CombinationStorage implements Storage {

    private int bufferSize;

    private PersistentStorage persistent;

    private RingBuffer<RaftProto.Entry> buffer;

    private ReadWriteLock lock = new ReentrantReadWriteLock();

    private Lock readLock = lock.readLock();

    private Lock writeLock = lock.writeLock();

    private volatile long firstIndex = Constants.DEFAULT_INIT_OFFSET;

    private volatile long firstTerm = Constants.DEFAULT_INIT_TERM;

    public CombinationStorage(int bufferSize, Wal wal) {
        Preconditions.checkArgument(bufferSize > 0);
        Preconditions.checkNotNull(wal);
        this.bufferSize = bufferSize;
        this.buffer = new RingBuffer<>(bufferSize);
        this.persistent = new PersistentStorage(wal);
    }

    @Override
    public void append(List<RaftProto.Entry> entries) {
        if (entries.isEmpty()) {
            return;
        }

        writeLock.lock();
        try {
            for (RaftProto.Entry entry : entries) {
                buffer.append(entry.getIndex(), entry);
            }
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public long lastIndex() {
        readLock.lock();
        try {
            return buffer.highWatermark() == Constants.DEFAULT_INIT_OFFSET ? buffer.highWatermark() : persistent.lastIndex();
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public RaftProto.Entry entry(long index) {
        readLock.lock();
        try {
            if (index < buffer.lowWatermark() || index > buffer.highWatermark()) {
                log.debug("search entry with index {} from wal, buffer state [{}, {}]", index, buffer.lowWatermark(), buffer.highWatermark());
                return persistent.entry(index);
            }

            return buffer.entry(index);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public List<RaftProto.Entry> entries(long begin, long end) {
        readLock.lock();
        try {
            long lowWatermark = buffer.lowWatermark();
            long highWatermark = Math.min(buffer.highWatermark(), end - 1);
            if (begin >= lowWatermark) {
                return buffer.entries(begin, highWatermark);
            } else if (begin < lowWatermark && end >= lowWatermark) {
                List<RaftProto.Entry> entries = Lists.newArrayList();
                entries.addAll(persistent.entries(begin, lowWatermark));
                entries.addAll(buffer.entries(lowWatermark, highWatermark));
                return entries;
            } else {
                return persistent.entries(begin, end);
            }
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public long firstIndex() {
        return firstIndex;
    }

    @Override
    public long firstTerm() {
        return firstTerm;
    }

    @Override
    public void updateSnapshotMetadata(RaftProto.SnapshotMetadata metadata) {
        this.firstIndex = metadata.getIndex();
        this.firstTerm = metadata.getTerm();
    }

    @Override
    public void truncateSuffix(long startOffset) throws IOException {
        writeLock.lock();
        try {
            buffer.truncate(startOffset - 1);
            persistent.truncateSuffix(startOffset);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void truncatePrefix(long endOffset) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void truncatePrefixAndUpdateMeta(long endOffset, RaftProto.SnapshotMetadata metadata) {
        updateSnapshotMetadata(metadata);
    }

    @Override
    public void recover() {
        persistent.recover();
        long startOffset = Math.max(persistent.lastIndex() - bufferSize, firstIndex);
        long recoverOffset = startOffset;
        try {
            writeLock.lock();
            int recovers = 0;
            while (true) {
                List<RaftProto.Entry> entries = persistent.entries(startOffset, startOffset + Constants.DEFAULT_RECOVER_BATCH_SIZE);
                if (entries.isEmpty()) {
                    log.info("recover storage cache success, recover start offset {}, recover entries size {}", recoverOffset, recovers);
                    break;
                } else {
                    Preconditions.checkArgument(startOffset == entries.get(0).getIndex(), "illegal fetch entries by given recover start offset");
                }

                startOffset += entries.size();
                recovers += entries.size();
                for (RaftProto.Entry entry : entries) {
                    buffer.append(entry.getIndex(), entry);
                }
            }
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void clear() {
        writeLock.lock();
        try {
            buffer.clear();
        } finally {
            writeLock.unlock();
        }
    }
}
