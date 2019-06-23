package org.menina.raft.core;

import org.menina.raft.common.Constants;
import org.menina.raft.common.NodeInfo;
import org.menina.raft.common.RaftConfig;
import org.menina.raft.common.RaftThread;
import org.menina.raft.common.task.PurgeTask;
import org.menina.raft.common.task.SentinelTask;
import org.menina.raft.core.loop.ApplyEventLoop;
import org.menina.raft.core.loop.GroupCommitLoop;
import org.menina.raft.core.loop.RaftEventLoop;
import org.menina.raft.election.LogicalClock;
import org.menina.raft.message.RaftProto;
import org.menina.raft.statemachine.StateMachine;
import org.menina.raft.transport.Transporter;
import  org.menina.rail.client.ConnectStateListener;
import  org.menina.rail.client.Reference;
import  org.menina.rail.config.ClientOptions;
import  org.menina.rail.config.ServerOptions;
import  org.menina.rail.server.ExporterServer;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * @author zhenghao
 * @date 2019/4/11
 */
@Slf4j
public class RaftNode extends AbstractRaftNode {

    public RaftNode(RaftConfig config, StateMachine stateMachine) {
        super(config, stateMachine);
    }

    @Override
    public void start() {
        backend();
        network();
        eventLoop();
        globalClock();
        background();
    }

    private void backend() {
        try {
            this.snapshotter.recover();
            RaftProto.Snapshot snapshot = this.snapshotter.loadNewest();
            this.nextOffsetMetaData = this.wal.recoverWal();
            this.nodeInfo().setReplayState(ReplayState.REPLAYING);
            if (snapshot != null) {
                this.raftLog.updateSnapshotMetadata(snapshot.getMeta());
                this.raftLog.appliedTo(snapshot.getMeta().getIndex());
                log.info("start apply snapshot {} to state machine", snapshot.getMeta().getIndex());
                try {
                    nodeInfo().setSnapshotApplying(true);
                    this.stateMachine.applySnapshot(config.isSnapshotReadOnly()
                            ? snapshot.getData().asReadOnlyByteBuffer()
                            : ByteBuffer.wrap(snapshot.getData().toByteArray()));
                    log.info("apply snapshot {} to state machine success", snapshot.getMeta().getIndex());
                } finally {
                    nodeInfo().setSnapshotApplying(false);
                }
            } else if (wal.startIndex() != 0) {
                throw new IllegalStateException("Log is truncated but snapshot is lost, require at least one snapshot or purge all logs");
            }

            RaftProto.Entry latest = wal.lastEntry();
            this.recover(snapshot, latest);
            this.storage.recover();
            if (latest == null || (snapshot != null && latest.getIndex() <= snapshot.getMeta().getIndex())) {
                nodeInfo().setReplayState(ReplayState.REPLAYED);
                log.info("state machine replay success, replay state {}", nodeInfo().getReplayState());
            }
        } catch (IOException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    private void network() {
        NodeInfo local = this.cluster.get(config.getId());
        local.setTransporter(this.transporter);
        ServerOptions serverOptions = ServerOptions.builder()
                .port(local.getPort())
                .build();
        serverOptions.setThreadPoolSize(config.getIoThreadsNum());
        this.server = new ExporterServer(serverOptions);
        this.server.export(this.transporter);
        this.server.start();
        peers.values().iterator().forEachRemaining(new Consumer<NodeInfo>() {
            @Override
            public void accept(NodeInfo nodeInfo) {
                ClientOptions clientOptions = ClientOptions.builder()
                        .remoteAddress(nodeInfo.getHost())
                        .port(nodeInfo.getPort())
                        .lazyConnect(true)
                        .reconnectCheckIntervalMills(config.getReconnectTimeoutTick() * Constants.LOGIC_CLOCK_ACCURACY_MILLS)
                        .connectStateListener(new ConnectStateListener() {
                            @Override
                            public void onConnected(String address, int port) {
                                nodeInfo.setDisconnected(false);
                                nodeInfo.setPaused(false);
                                nodeInfo.setTransportSnapshot(false);
                                nodeInfo.setPromote(true);
                                mayRefreshState(false);
                                log.info("(re-)connect to node {} success, current group state [{}]", nodeInfo.getId(), groupState);
                            }

                            @Override
                            public void onDisconnected(String address, int port) {
                                nodeInfo.setPaused(true);
                                nodeInfo.setDisconnected(true);
                                nodeInfo.setPromote(false);
                                mayRefreshState(false);
                                log.info("disconnect with node {}, current group state [{}]", nodeInfo.getId(), groupState);
                            }
                        }).build();
                Reference reference = new Reference(clientOptions);
                nodeInfo.setTransporter(reference.refer(Transporter.class));
            }
        });
    }

    private void eventLoop() {
        RaftThread.daemon(new GroupCommitLoop(requestChannel, this), Constants.DEFAULT_GROUP_COMMIT_THREAD).start();
        RaftThread.daemon(new RaftEventLoop(requestChannel, this), Constants.DEFAULT_RAFT_EVENT_LOOP_THREAD).start();
        RaftThread.daemon(new ApplyEventLoop(requestChannel, this), Constants.DEFAULT_APPLY_EVENT_LOOP_THREAD).start();
    }

    private void globalClock() {
        this.clock = new LogicalClock(config.getClockAccuracyMills());
        this.clock.addListener(this.electionTick);
        this.becomeFollower(term, null);
        this.clock.start();
    }

    private void background() {
        backgroundExecutor.scheduleAtFixedRate(new PurgeTask(this), 0, config.getPurgeIntervalSeconds(), TimeUnit.SECONDS);
        backgroundExecutor.scheduleAtFixedRate(new SentinelTask(this), 0, config.getSentinelCheckIntervalSeconds(), TimeUnit.SECONDS);
    }
}
