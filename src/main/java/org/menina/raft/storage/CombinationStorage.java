package org.menina.raft.storage;

import org.menina.raft.message.RaftProto;

import java.io.IOException;
import java.util.List;

/**
 * @author zhenghao
 * @date 2019/4/17
 */
public class CombinationStorage implements Storage {

    @Override
    public void append(List<RaftProto.Entry> entries) {

    }

    @Override
    public long lastIndex() {
        return 0;
    }

    @Override
    public RaftProto.Entry entry(long index) {
        return null;
    }

    @Override
    public List<RaftProto.Entry> entries(long begin, long end) {
        return null;
    }

    @Override
    public long firstIndex() {
        return 0;
    }

    @Override
    public long firstTerm() {
        return 0;
    }

    @Override
    public void updateSnapshotMetadata(RaftProto.SnapshotMetadata metadata) {

    }

    @Override
    public void truncateSuffix(long startOffset) throws IOException {

    }

    @Override
    public void truncatePrefix(long endOffset) {

    }

    @Override
    public void truncatePrefixAndUpdateMeta(long endOffset, RaftProto.SnapshotMetadata metadata) {

    }

    @Override
    public void recover() {

    }

    @Override
    public void clear() {

    }
}
