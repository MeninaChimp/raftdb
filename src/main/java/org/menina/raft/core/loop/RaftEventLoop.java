package org.menina.raft.core.loop;

import com.google.common.base.Preconditions;
import org.menina.raft.api.Node;
import org.menina.raft.api.RaftApis;
import org.menina.raft.common.Constants;
import org.menina.raft.common.Event;
import org.menina.raft.common.NodeInfo;
import org.menina.raft.common.Ready;
import org.menina.raft.core.DefaultRaftApis;
import org.menina.raft.core.RequestChannel;
import org.menina.raft.message.RaftProto;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;

/**
 * @author zhenghao
 * @date 2019/2/18
 */
@Slf4j
@NotThreadSafe
public class RaftEventLoop implements EventLoop {

    private RaftApis raftApis;
    private RequestChannel requestChannel;
    private Node raftNode;
    private boolean running = true;
    private int yieldMills;
    private boolean canAdvance;
    private long lastDriveConsistent;

    public RaftEventLoop(RequestChannel requestChannel, Node raftNode) {
        Preconditions.checkNotNull(requestChannel);
        Preconditions.checkNotNull(raftNode);
        this.raftNode = raftNode;
        this.requestChannel = requestChannel;
        this.raftApis = new DefaultRaftApis(raftNode);
        this.yieldMills = raftNode.config().getRaftEventLoopYieldMills();
    }

    @Override
    public void run() {
        while (running) {
            try {
                if (requestChannel.canFetchMessage()) {
                    RaftProto.Message message = (RaftProto.Message) requestChannel.poll(RaftProto.EventType.MESSAGE, 0, TimeUnit.MILLISECONDS);
                    raftApis.handleEvent(message);
                } else if (raftNode.isLeader()) {
                    raftNode.peers().values().iterator().forEachRemaining(new Consumer<NodeInfo>() {
                        @Override
                        public void accept(NodeInfo nodeInfo) {
                            if (allowDriveConsistent(lastDriveConsistent)
                                    && !nodeInfo.isDisconnected()
                                    && !nodeInfo.isPaused()
                                    && nodeInfo.getMatchIndex() != 0
                                    && nodeInfo.getMatchIndex() != raftNode.raftLog().lastIndex()
                                    && !nodeInfo.isTransportSnapshot()) {
                                // processing node offline driver cluster can be written
                                log.debug("leader {} with node {} log is not consistent, send an additional broadcast",
                                        raftNode.config().getId(), nodeInfo.getId());
                                lastDriveConsistent = raftNode.clock().now();
                                raftApis.handleEvent(RaftProto.Message.newBuilder()
                                        .setType(RaftProto.MessageType.ENTRY_BROADCAST)
                                        .setTerm(raftNode.currentTerm())
                                        .setFrom(raftNode.config().getId())
                                        .build());
                            } else if (nodeInfo.isPromote()) {
                                log.info("node {} re-connect is detected, need to send an additional broadcast to check if a data loss has occurred.", nodeInfo.getId());
                                nodeInfo.setPromote(false);
                                raftApis.handleEvent(RaftProto.Message.newBuilder()
                                        .setType(RaftProto.MessageType.ENTRY_BROADCAST)
                                        .setTerm(raftNode.currentTerm())
                                        .setFrom(raftNode.config().getId())
                                        .build());

                            }
                        }
                    });
                }

                if (requestChannel.canFetchPropose()) {
                    RaftProto.Message propose = (RaftProto.Message) requestChannel.poll(RaftProto.EventType.PROPOSAL, 0, TimeUnit.MILLISECONDS);
                    raftApis.handleEvent(propose);
                }

                // do not adjust the order, that will result in loss of signal
                if (!requestChannel.isReady() && !requestChannel.isAdvance() && requestChannel.isCanCommit()) {
                    Ready ready = raftApis.newReady();
                    if (ready.validate()) {
                        canAdvance = true;
                        requestChannel.setReady(true);
                        requestChannel.publish(Event.builder()
                                .type(RaftProto.EventType.READY)
                                .value(ready)
                                .build());

                        broadcastReady();
                    }
                }

                if (requestChannel.isAdvance() && canAdvance) {
                    canAdvance = false;
                    raftNode.raftLog().stableTo(requestChannel.getStableTo());
                    raftNode.raftLog().commitTo(raftNode.nodeInfo().getCommitted());
                    requestChannel.setAdvance(false);
                }

                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(yieldMills));
            } catch (Throwable t) {
                log.error(t.getMessage(), t);
            }
        }
    }

    @Override
    public void close() {
        running = false;
    }

    private void broadcastReady() {
        requestChannel.getReadyLock().lock();
        try {
            requestChannel.getReadySemaphore().signalAll();
        } finally {
            requestChannel.getReadyLock().unlock();
        }
    }

    private boolean allowDriveConsistent(long clock) {
        return raftNode.clock().now() - clock >= Constants.DEFAULT_DRIVE_CONSISTENT_INTERVAL;
    }
}
