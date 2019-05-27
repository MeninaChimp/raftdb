package org.menina.raft.core.loop;

import com.google.common.base.Preconditions;
import org.menina.raft.api.Node;
import org.menina.raft.api.RaftApis;
import org.menina.raft.common.Apply;
import org.menina.raft.core.DefaultRaftApis;
import org.menina.raft.core.RequestChannel;
import org.menina.raft.message.RaftProto;
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

    public ApplyEventLoop(RequestChannel requestChannel, Node raftNode) {
        Preconditions.checkNotNull(requestChannel);
        Preconditions.checkNotNull(raftNode);
        this.raftNode = raftNode;
        this.requestChannel = requestChannel;
        this.raftApis = new DefaultRaftApis(raftNode);
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
                        }
                    }

                    if (apply.getSnapshot() != null) {
                        RaftProto.Snapshot snapshot = apply.getSnapshot();
                        raftNode.snapshotter().save(snapshot);
                        raftNode.raftLog().appliedTo(snapshot.getMeta().getIndex());
                        raftNode.recover(snapshot, null);
                        log.info("start to apply snapshot {} to state machine", snapshot.getMeta().getIndex());
                        raftNode.stateMachine().applySnapshot(raftNode.config().isSnapshotReadOnly()
                                ? snapshot.getData().asReadOnlyByteBuffer()
                                : ByteBuffer.wrap(snapshot.getData().toByteArray()));
                        log.info("apply snapshot {} to state machine success", snapshot.getMeta().getIndex());
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
