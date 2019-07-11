package org.menina.raft.common;

import java.io.File;

/**
 * @author zhenghao
 * @date 2019/1/23
 */
public class Constants {

    public enum StorageType {
        /**
         * disk storage
         */
        DISK,

        /**
         * memory storage
         */
        MEMORY,

        /**
         * disk + memory storage
         */
        COMBINATION
    }

    public static final String HEARTBEAT_TICK = "HEARTBEAT_TICK";

    public static final String ELECTION_TICK = "ELECTION_TICK";

    public static final String LEASE_TICK = "LEASE_TICK";

    public static final int LOGIC_CLOCK_ACCURACY_MILLS = 1000;

    public static final int DEFAULT_ELECTION_TIMEOUT_TICK = 5;

    public static final int DEFAULT_HEARTBEAT_TIMEOUT_TICK = 2;

    public static final int DEFAULT_LEASE_TIMEOUT_TICK = DEFAULT_HEARTBEAT_TIMEOUT_TICK << 1;

    public static final int DEFAULT_RECONNECT_TICK = DEFAULT_ELECTION_TIMEOUT_TICK - 1;

    public static final String ADDRESS_SEPARATOR = " ";

    public static final int NOT_VOTE = -1;

    public static final int MAX_PENDING_PROPOSAL = 0x2000;

    public static final int RECEIVE_BUF_SIZE = 0x2000;

    public static final int MAX_READY_SIZE = 1;

    public static final int MAX_APPLY_SIZE = 0x40;

    public static final int DEFAULT_BATCH_SIZE = 0x200;

    public static final int DEFAULT_APPLY_BATCH_SIZE = DEFAULT_BATCH_SIZE << 2;

    public static final int DEFAULT_RECOVER_BATCH_SIZE = DEFAULT_BATCH_SIZE << 4;

    public static final int MAX_ENTRY_SIZE = 0x400000;

    public static final long SEGMENT_INDEX_SIZE = 0x400000L;

    public static final long SEGMENT_SIZE = 0x20000000L;

    public static final int INDEX_INTERVAL_BYTES = 0x1000;

    public static final long SEGMENT_ROLL_INTERVAL = 3 * 24 * 60 * 60 * 1000;

    public static final String DEFAULT_DATA_DIR = File.separator + "data";

    public static final String DEFAULT_WAL_DIR = DEFAULT_DATA_DIR + File.separator + "wal";

    public static final String DEFAULT_SNAP_DIR = DEFAULT_DATA_DIR + File.separator + "snap";

    public static final String LOG_SUFFIX = "log";

    public static final String INDEX_SUFFIX = "index";

    public static final String SNAP_SUFFIX = "snap";

    public static final String TEMP_SUFFIX = "temp";

    public static final String SNAP_TEMP_SUFFIX = SNAP_SUFFIX + "." + TEMP_SUFFIX;

    public static final boolean DIRTY_DATA_CHECK_ENABLE = false;

    public static final long DEFAULT_INIT_OFFSET = -1;

    public static final long DEFAULT_INIT_TERM = -1;

    public static final int DEFAULT_RAFT_EVENT_LOOP_YIELD_MILLS = 0;

    public static final boolean DEFAULT_LOG_FLUSH_ENABLE = false;

    public static final int DEFAULT_APPEND_TIMEOUT_MILLS = 0;

    public static final int DEFAULT_GROUP_UNAVAILABLE_TIMEOUT_MILLS = Integer.MAX_VALUE;

    public static final StorageType DEFAULT_STORAGE_TYPE = StorageType.COMBINATION;

    public static final int DEFAULT_NUM_IO_THREADS = 20;

    public static final int DEFAULT_MAX_SNAPSHOT_LAG_SIZE = 0x800000;

    public static final int DEFAULT_BACKGROUND_THREADS = 1;

    public static final int DEFAULT_SNAPSHOT_TRIGGER_CHECK_INTERVAL_SECONDS = 30;

    public static final int DEFAULT_GROUP_COMMIT_THREAD_TIMEOUT_SECONDS = 5;

    public static final int DEFAULT_APPLY_THREAD_TIMEOUT_SECONDS = DEFAULT_SNAPSHOT_TRIGGER_CHECK_INTERVAL_SECONDS << 1;

    public static final int DEFAULT_SENTINEL_CHECK_INTERVAL_SECONDS = DEFAULT_GROUP_COMMIT_THREAD_TIMEOUT_SECONDS << 1;

    public static final int DEFAULT_DRIVE_CONSISTENT_INTERVAL = 0;

    public static final int DEFAULT_MIN_SNAPSHOTS_RETENTION = 2;

    public static final int DEFAULT_PURGE_INTERVAL_SECONDS = 600;

    public static final int DEFAULT_RANDOM_ELECTION_TIMEOUT = 4;

    public static final int DEFAULT_RING_BUFFER_SIZE = 0x1000;

    public static final boolean DEFAULT_SNAPSHOT_READ_ONLY = true;

    public static final int DEFAULT_REPLAY_BATCH_SIZE = 1 << 14;

    public static final String DEFAULT_RAFT_CONFIG_PATH = "RAFT_CONFIG_PATH";

    public static final String DEFAULT_CONFIG_FILE_NAME = "raft.yml";

    public static final String DEFAULT_GROUP_COMMIT_THREAD = "group-commit-event-loop-thread";

    public static final String DEFAULT_RAFT_EVENT_LOOP_THREAD = "raft-event-loop-thread";

    public static final String DEFAULT_APPLY_EVENT_LOOP_THREAD = "apply-event-loop-thread";

    public static final String DEFAULT_SCHEDULE_BACKGROUND_THREAD = "schedule-background-thread";

    public static final String DEFAULT_TICK_EVENT_LOOP_THREAD = "tick-event-loop-thread";

}
