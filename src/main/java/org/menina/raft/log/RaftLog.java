package org.menina.raft.log;

import com.google.common.base.Preconditions;
import org.menina.raft.api.Node;
import org.menina.raft.common.Constants;
import org.menina.raft.message.RaftProto;
import org.menina.raft.storage.Storage;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author zhenghao
 * @date 2019/1/28
 * <p>
 * |-----------------|--------------------------------------------|-------------------|
 * |   snapshot      |                  storage                   |    unstable_log   |
 * |-----------------|--------------------------------------------|-------------------|
 * ````````firstIndex^     applied^          committed^     offset^       lastLogIndex^
 */
@Data
@Slf4j
@NotThreadSafe
public class RaftLog implements Log {

    private Node raftNode;

    private UnstableLog unstableLog;

    private Storage storage;

    private long committed = Constants.DEFAULT_INIT_OFFSET;

    private long applied = Constants.DEFAULT_INIT_OFFSET;

    public RaftLog(Node raftNode) {
        Preconditions.checkNotNull(raftNode);
        this.raftNode = raftNode;
        this.storage = raftNode.storage();
        this.unstableLog = new UnstableLog();
    }

    @Override
    public void append(List<RaftProto.Entry> entries) {
        unstableLog.append(entries);
    }

    @Override
    public long lastIndex() {
        long lastIndex = unstableLog.lastIndex();
        if (lastIndex == Constants.DEFAULT_INIT_OFFSET) {
            return storage.lastIndex();
        }

        return lastIndex;
    }

    @Override
    public long term(long index) {
        RaftProto.Entry entry = entry(index);
        if (entry != null) {
            return entry.getTerm();
        }

        if (index == firstIndex()) {
            return firstTerm();
        }

        return Constants.DEFAULT_INIT_TERM;
    }

    @Override
    public boolean matchTerm(long index, long term) {
        return index <= lastIndex() && term(index) == term;
    }

    @Override
    public boolean isMoreUpToDate(long lastLogTerm, long lastLogIndex) {
        Preconditions.checkNotNull(lastLogTerm);
        Preconditions.checkNotNull(lastLogIndex);
        RaftProto.Entry last = entry(lastIndex());
        if (last == null) {
            last = RaftProto.Entry
                    .newBuilder()
                    .setTerm(Constants.DEFAULT_INIT_TERM)
                    .setIndex(Constants.DEFAULT_INIT_OFFSET)
                    .build();
        }
        return lastLogTerm > last.getTerm() || (lastLogTerm == last.getTerm() && lastLogIndex >= last.getIndex());
    }

    @Override
    public RaftProto.Entry entry(long index) {
        if (index <= firstIndex() || index > lastIndex()) {
            return null;
        }

        if (index > unstableLog.offset() && index < unstableLog.lastIndex()) {
            return unstableLog.entry(index);
        } else {
            return storage.entry(index);
        }
    }

    @Override
    public List<RaftProto.Entry> entries(long startIndex, long limitSize) {
        long offset = unstableLog.offset();
        List<RaftProto.Entry> entries = new ArrayList<>();
        long endIndex = lastIndex() > startIndex + limitSize ? startIndex + limitSize : lastIndex();
        if (startIndex <= endIndex) {
            if (startIndex > offset) {
                log.debug("fetch entries from unstable log, startIndex {}, endIndex {}", startIndex, endIndex);
                return unstableLog.entries(startIndex, endIndex + 1);
            } else if (startIndex <= offset && endIndex > offset) {
                log.debug("fetch entries from unstable log and storage, startIndex {}, offset {}, endIndex {}", startIndex, offset, endIndex);
                entries.addAll(storage.entries(startIndex, offset + 1));
                entries.addAll(unstableLog.entries(offset + 1, endIndex));
            } else {
                // active when log alignment occurs
                log.debug("fetch entries from storage");
                entries.addAll(storage.entries(startIndex, startIndex + limitSize));
            }
        } else {
            log.debug("fetch entries from storage");
            entries.addAll(storage.entries(startIndex, startIndex + limitSize));
        }

        return entries;
    }

    @Override
    public List<RaftProto.Entry> nextCommittedEntries(int limitSize) {
        long lowWaterMark = Math.max(storage.firstIndex(), applied);
        if (!raftNode.nodeInfo().isApplying() && committed > lowWaterMark) {
            List<RaftProto.Entry> entries = entries(applied + 1, Math.min(limitSize, committed - lowWaterMark));
            if (entries.size() > 0) {
                raftNode.nodeInfo().setApplying(true);
                log.debug("nextCommittedEntries, applied {}, committed {}, fetch size {}", applied, committed, entries.size());
                return entries;
            }
        }

        return null;
    }

    @Override
    public long committed() {
        return committed;
    }

    @Override
    public long applied() {
        return this.applied;
    }

    @Override
    public List<RaftProto.Entry> unstableLogs() {
        return unstableLog.entries();
    }

    @Override
    public void commitTo(long offset) {
        if (offset > committed) {
            this.committed = offset;
        }
    }

    @Override
    public void appliedTo(long offset) {
        if (offset > applied) {
            this.applied = offset;
        }
    }

    @Override
    public long checkPrefixEntry(long index, long term) {
        long lastIndex = lastIndex();
        if (index > lastIndex) {
            log.info("leader {} append entries prefix index {} exceed current node {} last index {}, response last index to leader so let retry",
                    raftNode.leader().getId(),
                    index,
                    raftNode.config().getId(),
                    lastIndex);

            return lastIndex;
        }

        if (index == firstIndex() && index == firstTerm()) {
            return index;
        }

        long prefixTerm = term(index);
        if (term != prefixTerm) {
            log.info("found conflict for append entry prefix index {} term {}, exist term with {}, leader {}, current node {}, may have encountered a leadership switch and need log truncation",
                    index,
                    term,
                    prefixTerm,
                    raftNode.leader().getId(),
                    raftNode.config().getId());

            return index - 1;
        }

        return index;
    }

    @Override
    public boolean maySendSnapshot(long offset) {
        return offset <= storage.firstIndex();
    }

    @Override
    public void stableTo(long offset) {
        if (offset > unstableLog.offset()) {
            unstableLog.updateOffset(offset);
            unstableLog.truncate(offset);
        }
    }

    @Override
    public boolean truncate(long startOffset) {
        Preconditions.checkArgument(startOffset <= lastIndex());
        Preconditions.checkArgument(startOffset >= 0);
        long offset = unstableLog.offset();
        if (startOffset > offset && offset != Constants.DEFAULT_INIT_OFFSET) {
            unstableLog.clear();
            return true;
        }

        try {
            unstableLog.clear();
            storage.truncateSuffix(startOffset);
        } catch (IOException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }

        return true;
    }

    @Override
    public long firstIndex() {
        return storage.firstIndex();
    }

    @Override
    public long firstTerm() {
        return storage.firstTerm();
    }

    @Override
    public void updateSnapshotMetadata(RaftProto.SnapshotMetadata metadata) {
        storage.updateSnapshotMetadata(metadata);
    }

    @Override
    public void submitSnapshot(RaftProto.Snapshot snapshot) {
        if (snapshot != null) {
            unstableLog.clear();
            updateSnapshotMetadata(snapshot.getMeta());
        }

        unstableLog.setSnapshot(snapshot);
    }

    @Override
    public RaftProto.Snapshot snapshot() {
        return unstableLog.snapshot();
    }
}
