package org.menina.raft.common.task;

import com.google.common.base.Preconditions;
import org.menina.raft.api.Node;
import org.menina.raft.common.Constants;
import org.menina.raft.common.RaftUtils;
import org.menina.raft.message.RaftProto;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.util.Map;
import java.util.NavigableMap;

/**
 * @author zhenghao
 * @date 2019/4/11
 */
@Slf4j
public class PurgeTask implements Runnable {

    private Node raftNode;

    public PurgeTask(Node raftNode) {
        Preconditions.checkNotNull(raftNode);
        this.raftNode = raftNode;
    }

    @Override
    public void run() {
        try {
            log.info("start purge snapshot and wal");
            if (raftNode.nodeInfo().isSnapshot()) {
                log.info("currently creating a snapshot, skip purging");
                return;
            }

            NavigableMap<Long, RaftProto.SnapshotMetadata> snapshots = raftNode.snapshotter().snapshots();
            if (snapshots.isEmpty()) {
                log.info("interrupt purge task for empty snaps");
                return;
            }

            int minRetention = raftNode.config().getMinSnapshotsRetention();
            Preconditions.checkArgument(minRetention >= Constants.DEFAULT_MIN_SNAPSHOTS_RETENTION);
            long retention = snapshots.size();
            long firstIndex = Long.MAX_VALUE;
            for (int i = 0; i < minRetention; i++) {
                Map.Entry<Long, RaftProto.SnapshotMetadata> entry = snapshots.pollLastEntry();
                if (entry != null) {
                    firstIndex = entry.getKey();
                }
            }

            int snapPurged = 0;
            if (snapshots.size() == 0) {
                log.info("no need to purge snaps, current file count {}, min retention {}", retention, minRetention);
            } else {
                for (RaftProto.SnapshotMetadata metadata : snapshots.values()) {
                    File snap = RaftUtils.snapFile(raftNode.config().getSnap(), metadata.getTerm(), metadata.getIndex());
                    if (!snap.delete()) {
                        log.info("fail to purge {}, check permission or disk status please", snap);
                    } else {
                        snapPurged++;
                        log.info("purge {} success", snap);
                    }
                }
            }

            int logPurged = raftNode.wal().purge(firstIndex);
            log.info("purge snapshot and wal success, snapshot purged count {}, log purged count {}", snapPurged, logPurged);
        } catch (Throwable t) {
            log.error(t.getMessage(), t);
        }
    }
}