package org.menina.raft.storage;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.menina.raft.common.Constants;
import org.menina.raft.message.RaftProto;
import org.menina.raft.wal.Wal;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.concurrent.ThreadSafe;
import java.io.IOException;
import java.util.ArrayList;

import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author zhenghao
 * @date 2019/2/25
 */
@Slf4j
@ThreadSafe
public class MemoryStorage implements Storage {

    private Wal wal;

    private List<RaftProto.Entry> entries = new ArrayList<>();

    private ReadWriteLock lock = new ReentrantReadWriteLock();

    private Lock readLock = lock.readLock();

    private Lock writeLock = lock.writeLock();

    private volatile long firstIndex = Constants.DEFAULT_INIT_OFFSET;

    private volatile long firstTerm = Constants.DEFAULT_INIT_TERM;

    public MemoryStorage(Wal wal) {
        Preconditions.checkNotNull(wal);
        this.wal = wal;
    }

    @Override
    public void append(List<RaftProto.Entry> entries) {
        try {
            writeLock.lock();
            this.entries.addAll(entries);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public long lastIndex() {
        try {
            readLock.lock();
            return entries.size() == 0 ? firstIndex() : entries.get(entries.size() - 1).getIndex();
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public RaftProto.Entry entry(long index) {
        try {
            readLock.lock();
            return index == firstIndex() ? null : entries.get((int) relativeOffset(index));
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public List<RaftProto.Entry> entries(long begin, long end) {
        Preconditions.checkArgument(begin > firstIndex());
        try {
            readLock.lock();
            return Lists.newArrayList(entries.subList((int) relativeOffset(begin), (int) relativeOffset(Math.min(lastIndex() + 1, end))));
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
        try {
            writeLock.lock();
            this.firstIndex = metadata.getIndex();
            this.firstTerm = metadata.getTerm();
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void truncateSuffix(long startOffset) throws IOException {
        if (startOffset > firstIndex()) {
            try {
                writeLock.lock();
                entries = entries.subList(0, (int) relativeOffset(startOffset));
                wal.truncate(startOffset);
            } finally {
                writeLock.unlock();
            }
        }
    }

    @Override
    public void truncatePrefix(long endOffset) {
        if (endOffset > firstIndex()) {
            try {
                writeLock.lock();
                entries = entries.subList((int) relativeOffset(endOffset) + 1, entries.size());
            } finally {
                writeLock.unlock();
            }
        }
    }

    @Override
    public void truncatePrefixAndUpdateMeta(long endOffset, RaftProto.SnapshotMetadata metadata) {
        if (endOffset > firstIndex()) {
            try {
                writeLock.lock();
                entries = entries.subList((int) relativeOffset(endOffset) + 1, entries.size());
                this.firstIndex = metadata.getIndex();
                this.firstTerm = metadata.getTerm();
            } finally {
                writeLock.unlock();
            }
        }
    }

    @Override
    public void recover() {
        Preconditions.checkArgument(wal.state().equals(Wal.State.AVAILABLE));
        long startOffset = firstIndex() + 1;
        long recoverOffset = startOffset;
        try {
            writeLock.lock();
            while (true) {
                List<RaftProto.Entry> entries = wal.fetch(startOffset, Constants.DEFAULT_RECOVER_BATCH_SIZE);
                if (entries.size() == 0) {
                    log.info("recover memory storage success, recover start offset {}, recover entries size {}", recoverOffset, this.entries.size());
                    break;
                } else {
                    Preconditions.checkArgument(startOffset == entries.get(0).getIndex(), "illegal fetch entries by given recover start offset");
                }

                startOffset += entries.size();
                this.entries.addAll(entries);
            }
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void clear() {
        try {
            writeLock.lock();
            this.entries.clear();
        } finally {
            writeLock.unlock();
        }
    }

    private long relativeOffset(long offset) {
        try {
            readLock.lock();
            return offset - (firstIndex() + 1);
        } finally {
            readLock.unlock();
        }
    }
}
