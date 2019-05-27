package org.menina.raft.storage;

import com.google.common.base.Preconditions;
import org.menina.raft.common.Constants;
import org.menina.raft.message.RaftProto;
import org.menina.raft.wal.Wal;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.List;

/**
 * @author zhenghao
 * @date 2019/2/27
 * <p>
 * persistent storage, delegated to wal, motivation is to reduce memory dependence
 */
@Slf4j
public class PersistentStorage implements Storage {

    private Wal wal;

    private RaftProto.SnapshotMetadata metadata = RaftProto.SnapshotMetadata.newBuilder()
            .setIndex(Constants.DEFAULT_INIT_OFFSET)
            .setTerm(Constants.DEFAULT_INIT_TERM)
            .build();

    public PersistentStorage(Wal wal) {
        Preconditions.checkNotNull(wal);
        this.wal = wal;
    }

    @Override
    public void append(List<RaftProto.Entry> entries) {
        wal.write(entries);
    }

    @Override
    public long lastIndex() {
        long index = wal.lastIndex();
        return index == Constants.DEFAULT_INIT_OFFSET ? firstIndex() : index;
    }

    @Override
    public RaftProto.Entry entry(long index) {
        List<RaftProto.Entry> entries = entries(index, index + 1);
        return entries.size() == 1 ? entries.get(0) : null;
    }

    @Override
    public List<RaftProto.Entry> entries(long begin, long end) {
        Preconditions.checkArgument(end >= begin);
        return wal.fetch(begin, (int) (end - begin));
    }

    @Override
    public long firstIndex() {
        return this.metadata.getIndex();
    }

    @Override
    public long firstTerm() {
        return this.metadata.getTerm();
    }

    @Override
    public void updateSnapshotMetadata(RaftProto.SnapshotMetadata metadata) {
        Preconditions.checkNotNull(metadata);
        this.metadata = metadata;
    }

    @Override
    public void truncateSuffix(long startOffset) throws IOException {
        wal.truncate(startOffset);
    }

    @Override
    public void truncatePrefix(long endOffset) {
    }

    @Override
    public void truncatePrefixAndUpdateMeta(long endOffset, RaftProto.SnapshotMetadata metadata) {
        Preconditions.checkNotNull(metadata);
        this.metadata = metadata;
    }

    @Override
    public void recover() {
        Preconditions.checkArgument(wal.state().equals(Wal.State.AVAILABLE));
    }

    @Override
    public void clear() throws IOException {
        wal.truncate(0);
    }
}
