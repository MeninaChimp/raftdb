默认配置：

```
# current node id
id: 1
# cluster info, structure IP:Port:Node id
cluster: 127.0.0.1:1011:1 127.0.0.1:1012:2 127.0.0.1:1013:3
# data dir
wal: \data\wal\1
# storage type [DISK, MEMORY]
storageType: MEMORY
# snapshot dir
snap: \data\snap\1
# global clock accuracy, unit：Mills
clockAccuracyMills: 1000
# heartbeat timeout
heartbeatTimeoutTick: 2
# election timeout
electionTimeoutTick: 5
# re-connect interval
reconnectTimeoutTick: 4
# if true, all data files are scanned at startup. If corruption occurs, all logs will be deleted from that offset.
enableDirtyCheck: false
# thread pool size for rpc
ioThreadsNum: 20
# thread pool size for back ground task
backgroundThreadsNum: 1
# if true, flush dish after each batch write
logFlushEnable: false
# index file size (4k)
maxIndexSize: 4194304
# max message size (8k)
maxMessageSize: 8192
# max segment size (100MB)
maxSegmentSize: 104857600
# max segment alive time (7 Day)
maxSegmentTime: 604800000
# the amount of messages written since the last snapshot, create a snapshot above the threshold
maxSnapshotLagSize: 1000
# min retention size for snapshot
minSnapshotsRetention: 2
# if enable pre-vote
preVote: true
# check interval if need to purge wal and snapshot
purgeIntervalSeconds: 30
# yield cpu, above zero will seriously affect throughput
raftEventLoopYieldMills: 0
# time to wait cluster available
raftGroupUnavailableTimeoutMills: 2147483647
# snapshot trigger interval, check if need to build snapshot
snapshotTriggerIntervalSeconds: 60
# snapshot read only
snapshotReadOnly: true
```
