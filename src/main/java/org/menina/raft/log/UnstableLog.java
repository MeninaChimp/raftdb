package org.menina.raft.log;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.menina.raft.common.Constants;
import org.menina.raft.message.RaftProto;
import org.menina.raft.snapshot.SnapshotFuture;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * @author zhenghao
 * @date 2019/2/16
 */
@Slf4j
@NotThreadSafe
public class UnstableLog {

    private long offset = Constants.DEFAULT_INIT_OFFSET;

    private List<RaftProto.Entry> entries = new ArrayList<>();

    private ConcurrentLinkedQueue<SnapshotFuture> snapshots = new ConcurrentLinkedQueue<>();

    public void addSnapshot(SnapshotFuture snapshot) {
        this.snapshots.add(snapshot);
    }

    public SnapshotFuture pollSnapshot() {
        return this.snapshots.poll();
    }

    public void append(List<RaftProto.Entry> entries) {
        this.entries.addAll(entries);
    }

    public long lastIndex() {
        return lastIndex(false);
    }

    public long lastIndex(boolean stable) {
        if (offset != Constants.DEFAULT_INIT_OFFSET || stable) {
            return offset + entries.size();
        } else if (offset == Constants.DEFAULT_INIT_OFFSET && entries.size() > 0) {
            return entries.get(entries.size() - 1).getIndex();
        } else {
            return offset;
        }
    }

    public long offset() {
        return offset;
    }

    public RaftProto.Entry entry(long index) {
        return entries.get((int) (index - (offset + 1)));
    }

    public List<RaftProto.Entry> entries(long begin, long end) {
        if (entries.size() == 0) {
            return Lists.newArrayList();
        }

        if (end > lastIndex() + 1) {
            throw new IndexOutOfBoundsException();
        }

        if (begin < offset) {
            throw new IndexOutOfBoundsException();
        }

        return Lists.newArrayList(this.entries.subList((int) (begin - (offset + 1)), (int) (end - (offset + 1))));
    }

    public List<RaftProto.Entry> entries() {
        return Lists.newArrayList(entries);
    }

    void updateOffset(long newOffset) {
        this.offset = newOffset;
    }

    void truncate(long suffix) {
        Preconditions.checkArgument(suffix >= 0);
        if (entries.size() == 0) {
            return;
        }

        this.entries = Lists.newArrayList(entries.subList((int) (suffix - entries.get(0).getIndex() + 1), entries.size()));
    }

    void clear() {
        this.entries = Lists.newArrayList();
    }
}
