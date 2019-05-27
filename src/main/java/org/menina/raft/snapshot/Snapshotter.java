package org.menina.raft.snapshot;

import org.menina.raft.message.RaftProto;

import java.io.IOException;
import java.util.NavigableMap;

/**
 * @author zhenghao
 * @date 2019/2/18
 */
public interface Snapshotter {

    void save(RaftProto.Snapshot snapshot) throws IOException;

    RaftProto.Snapshot loadNewest() throws IOException;

    void recover();

    NavigableMap<Long, RaftProto.SnapshotMetadata> snapshots();
}
