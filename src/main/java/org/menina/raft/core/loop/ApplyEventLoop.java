package org.menina.raft.core.loop;

import com.google.common.base.Preconditions;
import org.menina.raft.api.Node;
import org.menina.raft.api.RaftApis;
import org.menina.raft.api.State;
import org.menina.raft.common.Apply;
import org.menina.raft.common.Constants;
import org.menina.raft.core.DefaultRaftApis;
import org.menina.raft.core.RequestChannel;
import org.menina.raft.election.ElectionListener;
import org.menina.raft.message.RaftProto;
import org.menina.raft.storage.PersistentStorage;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.concurrent.NotThreadSafe;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author zhenghao
 * @date 2019/4/11
 */
@Slf4j
@NotThreadSafe
public class ApplyEventLoop implements EventLoop {

    private Node raftNode;
    private RaftApis raftApis;
    private RequestChannel requestChannel;
    private boolean running = true;
    private long lowWaterMark = Constants.DEFAULT_INIT_OFFSET;
    private long replayAmount = 0;

    public ApplyEventLoop(RequestChannel requestChannel, Node raftNode) {
        Preconditions.checkNotNull(requestChannel);
        Preconditions.checkNotNull(raftNode);
        this.raftNode = raftNode;
        this.requestChannel = requestChannel;
        this.raftApis = new DefaultRaftApis(raftNode);
        this.raftNode.addElectionListener(new ElectionListener() {
            @Override
            public void transferTo(State.Status status) {
                if (status.equals(State.Status.LEADER)) {
                    lowWaterMark = raftNode.raftLog().lastIndex();
                }
            }
        });
    }

    @Override
    public void run() {
        while (running) {
            try {
                // process backlog of apply channel
                if (!requestChannel.canFetchApply()) {
                    ensureApply();
                }

                requestChannel.setCanApply(false);
                raftNode.nodeInfo().setApplyTick(raftNode.clock().now());
                Apply apply = (Apply) requestChannel.poll(RaftProto.EventType.APPLY, 0, TimeUnit.SECONDS);
                if (apply != null) {
                    List<RaftProto.Entry> committedEntries = apply.getCommittedEntries();
                    if (committedEntries != null && committedEntries.size() > 0) {
                        try {
                            // the user-implemented state machine is responsible for the complete processing of the data,
                            // and the module does not initiate a retry for processing the failed data.
                            raftNode.stateMachine().apply(committedEntries);
                        } catch (Throwable t) {
                            log.error("error on apply entries to state machine, the module will not initiate a retry for processing the failed data, error message: {}", t.getMessage(), t);
                        } finally {
                            RaftProto.Entry last = committedEntries.get(committedEntries.size() - 1);
                            log.debug("current node {} update apply index to {}", raftNode.nodeInfo().getId(), last.getIndex());
                            raftNode.raftLog().appliedTo(last.getIndex());
                            raftNode.nodeInfo().setApplying(false);
                            replayAmount += committedEntries.size();
                            // data consistent process when leader switchover
                            if (raftNode.nodeInfo().getReplayState().equals(State.ReplayState.REPLAYING)) {
                                if (replayAmount % Constants.DEFAULT_REPLAY_BATCH_SIZE == 0) {
                                    log.info("Replay progress: {}", replayAmount);
                                }

                                if (raftNode.isLeader() && last.getIndex() >= lowWaterMark) {
                                    raftNode.nodeInfo().setReplayState(State.ReplayState.REPLAYED);
                                    log.info("leader {} replay success, replay state {}", raftNode.nodeInfo().getId(), raftNode.nodeInfo().getReplayState());

                                } else if (!raftNode.isLeader() && last.getIndex() >= raftNode.nodeInfo().getCommitted()) {
                                    raftNode.nodeInfo().setReplayState(State.ReplayState.REPLAYED);
                                    log.info("follower {} state machine replay success, replay state {}", raftNode.nodeInfo().getId(), raftNode.nodeInfo().getReplayState());
                                }
                            }
                        }
                    }

                    if (apply.getSnapshot() != null) {
                        try {
                            RaftProto.Snapshot snapshot = apply.getSnapshot().getSnapshot();
                            raftNode.snapshotter().save(snapshot);
                            raftNode.raftLog().appliedTo(snapshot.getMeta().getIndex());
                            raftNode.recover(snapshot, null);
                            log.info("start to apply snapshot {} to state machine", snapshot.getMeta().getIndex());
                            raftNode.nodeInfo().setSnapshotApplying(true);
                            raftNode.stateMachine().applySnapshot(raftNode.config().isSnapshotReadOnly()
                                    ? snapshot.getData().asReadOnlyByteBuffer()
                                    : ByteBuffer.wrap(snapshot.getData().toByteArray()));
                            raftNode.wal().setFirstIndex(snapshot.getMeta().getIndex());
                            if (!(raftNode.storage() instanceof PersistentStorage)) {
                                raftNode.storage().clear();
                                log.info("drop storage cache success");
                            }

                            apply.getSnapshot().getFuture().complete(snapshot);
                        } catch (Exception t) {
                            apply.getSnapshot().getFuture().completeExceptionally(t);
                        } finally {
                            raftNode.nodeInfo().setSnapshotApplying(false);
                        }
                    }
                }

                raftApis.triggerToSnapshot();
            } catch (Throwable t) {
                log.error(t.getMessage(), t);
            }
        }
    }

    @Override
    public void close() {
        this.running = false;
    }

    private void ensureApply() throws InterruptedException {
        requestChannel.getApplyLock().lock();
        try {
            // lost wake up
            if (!requestChannel.isCanApply()) {
                requestChannel.getApplySemaphore().await(raftNode.config().getSnapshotTriggerCheckIntervalSeconds(), TimeUnit.SECONDS);
            }
        } finally {
            requestChannel.getApplyLock().unlock();
        }
    }
}
