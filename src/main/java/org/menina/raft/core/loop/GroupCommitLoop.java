package org.menina.raft.core.loop;

import com.google.common.base.Preconditions;
import org.menina.raft.api.Node;
import org.menina.raft.common.Apply;
import org.menina.raft.common.Event;
import org.menina.raft.common.NodeInfo;
import org.menina.raft.common.Ready;
import org.menina.raft.core.RequestChannel;
import org.menina.raft.message.RaftProto;
import org.menina.raft.storage.PersistentStorage;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * @author zhenghao
 * @date 2019/2/25
 */
@Slf4j
@Data
public class GroupCommitLoop implements EventLoop {

    private RequestChannel requestChannel;
    private Node raftNode;
    private boolean running = true;
    private Apply apply = new Apply();

    public GroupCommitLoop(RequestChannel requestChannel, Node raftNode) {
        Preconditions.checkNotNull(requestChannel);
        Preconditions.checkNotNull(raftNode);
        this.requestChannel = requestChannel;
        this.raftNode = raftNode;
    }

    @Override
    public void run() {
        while (running) {
            try {
                ensureReady();
                raftNode.nodeInfo().setGroupCommitTick(raftNode.clock().now());
                Ready ready = (Ready) requestChannel.poll(RaftProto.EventType.READY, 0, TimeUnit.MILLISECONDS);
                if (ready.getEntriesSize() > 0) {
                    raftNode.wal().write(ready.getEntries());
                    if (raftNode.config().isLogFlushEnable()) {
                        raftNode.wal().flush();
                    }

                    if (!(raftNode.storage() instanceof PersistentStorage)) {
                        raftNode.storage().append(ready.getEntries());
                    }

                    requestChannel.setStableTo(ready.getEntries().get(ready.getEntriesSize() - 1).getIndex());
                }

                if (ready.needApply()) {
                    apply.setCommittedEntries(ready.getCommittedEntries());
                    apply.setSnapshot(ready.getSnapshot());
                    apply.setHardState(ready.getState());
                    requestChannel.publish(Event.builder()
                            .type(RaftProto.EventType.APPLY)
                            .value(apply)
                            .build());
                    broadcastApply();
                }

                if (ready.getMessageSize() > 0) {
                    ready.getMessages().iterator().forEachRemaining(new Consumer<RaftProto.Message>() {
                        @Override
                        public void accept(RaftProto.Message message) {
                            NodeInfo nodeInfo = raftNode.nodeInfo(message.getTo());
                            if (!nodeInfo.isDisconnected()) {
                                nodeInfo.getTransporter().request(message);
                            }
                        }
                    });
                }

                requestChannel.setCanCommit(false);
                requestChannel.setAdvance(true);
                requestChannel.setReady(false);
            } catch (Throwable t) {
                log.error(t.getMessage(), t);
            }
        }
    }

    @Override
    public void close() {
        running = false;
    }

    private void ensureReady() throws InterruptedException {
        requestChannel.getReadyLock().lock();
        try {
            requestChannel.setCanCommit(true);
            requestChannel.getReadySemaphore().await();
        } finally {
            requestChannel.getReadyLock().unlock();
        }
    }

    private void broadcastApply() {
        requestChannel.getApplyLock().lock();
        try {
            requestChannel.setCanApply(true);
            requestChannel.getApplySemaphore().signalAll();
        } finally {
            requestChannel.getApplyLock().unlock();
        }
    }
}
