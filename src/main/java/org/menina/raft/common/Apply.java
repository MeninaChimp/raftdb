package org.menina.raft.common;

import org.menina.raft.message.RaftProto;
import org.menina.raft.snapshot.SnapshotFuture;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * @author zhenghao
 * @date 2019/4/11
 */
@Data
@NoArgsConstructor
public class Apply {

    private List<RaftProto.Entry> committedEntries;

    private SnapshotFuture snapshot;

    private HardState hardState;

}
