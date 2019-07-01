package org.menina.raft.common;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author zhenghao
 * @date 2019/1/23
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RaftConfig {

    private String cluster;

    @Builder.Default
    private int id = -1;

    @Builder.Default
    private boolean preVote = true;

    @Builder.Default
    private String wal = Constants.DEFAULT_WAL_DIR;

    @Builder.Default
    private String snap = Constants.DEFAULT_SNAP_DIR;

    @Builder.Default
    private long maxSegmentSize = Constants.SEGMENT_SIZE;

    @Builder.Default
    private long maxIndexSize = Constants.SEGMENT_INDEX_SIZE;

    @Builder.Default
    private long maxSegmentTime = Constants.SEGMENT_ROLL_INTERVAL;

    @Builder.Default
    private int clockAccuracyMills = Constants.LOGIC_CLOCK_ACCURACY_MILLS;

    @Builder.Default
    private int electionTimeoutTick = Constants.DEFAULT_ELECTION_TIMEOUT_TICK;

    @Builder.Default
    private int heartbeatTimeoutTick = Constants.DEFAULT_HEARTBEAT_TIMEOUT_TICK;

    @Builder.Default
    private int leaseTimeoutTick = Constants.DEFAULT_LEASE_TIMEOUT_TICK;

    @Builder.Default
    private int reconnectTimeoutTick = Constants.DEFAULT_RECONNECT_TICK;

    @Builder.Default
    private boolean enableDirtyCheck = Constants.DIRTY_DATA_CHECK_ENABLE;

    @Builder.Default
    private int maxMessageSize = Constants.MAX_ENTRY_SIZE;

    @Builder.Default
    private int ringBufferSize = Constants.DEFAULT_RING_BUFFER_SIZE;

    @Builder.Default
    private int raftEventLoopYieldMills = Constants.DEFAULT_RAFT_EVENT_LOOP_YIELD_MILLS;

    @Builder.Default
    private boolean logFlushEnable = Constants.DEFAULT_LOG_FLUSH_ENABLE;

    @Builder.Default
    private int raftGroupUnavailableTimeoutMills = Constants.DEFAULT_GROUP_UNAVAILABLE_TIMEOUT_MILLS;

    @Builder.Default
    private Constants.StorageType storageType = Constants.DEFAULT_STORAGE_TYPE;

    @Builder.Default
    private int ioThreadsNum = Constants.DEFAULT_NUM_IO_THREADS;

    @Builder.Default
    private int maxSnapshotLagSize = Constants.DEFAULT_MAX_SNAPSHOT_LAG_SIZE;

    @Builder.Default
    private int backgroundThreadsNum = Constants.DEFAULT_BACKGROUND_THREADS;

    @Builder.Default
    private int snapshotTriggerCheckIntervalSeconds = Constants.DEFAULT_SNAPSHOT_TRIGGER_CHECK_INTERVAL_SECONDS;

    @Builder.Default
    private int minSnapshotsRetention = Constants.DEFAULT_MIN_SNAPSHOTS_RETENTION;

    @Builder.Default
    private int purgeIntervalSeconds = Constants.DEFAULT_PURGE_INTERVAL_SECONDS;

    @Builder.Default
    private int sentinelCheckIntervalSeconds = Constants.DEFAULT_SENTINEL_CHECK_INTERVAL_SECONDS;

    @Builder.Default
    private int applyThreadTimeoutSeconds = Constants.DEFAULT_APPLY_THREAD_TIMEOUT_SECONDS;

    @Builder.Default
    private int groupCommitThreadTimeoutSeconds = Constants.DEFAULT_GROUP_COMMIT_THREAD_TIMEOUT_SECONDS;

    @Builder.Default
    private boolean snapshotReadOnly = Constants.DEFAULT_SNAPSHOT_READ_ONLY;

}
