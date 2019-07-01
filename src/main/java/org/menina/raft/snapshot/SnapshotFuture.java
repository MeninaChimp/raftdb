package org.menina.raft.snapshot;

import org.menina.raft.message.RaftProto;
import lombok.Builder;
import lombok.Getter;

import java.util.concurrent.CompletableFuture;

/**
 * @author zhenghao
 * @date 2019/6/4
 */
@Getter
@Builder
public class SnapshotFuture {

    private RaftProto.Snapshot snapshot;
    private CompletableFuture<RaftProto.Snapshot> future;

}
