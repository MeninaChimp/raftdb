package org.menina.raft;

import com.google.common.base.Preconditions;
import com.google.protobuf.UnsafeByteOperations;
import org.menina.raft.api.Endpoint;
import org.menina.raft.api.Node;
import org.menina.raft.api.Proposer;
import org.menina.raft.api.State;
import org.menina.raft.common.Constants;
import org.menina.raft.common.NodeInfo;
import org.menina.raft.common.RaftUtils;
import org.menina.raft.core.GroupStateListener;
import org.menina.raft.exception.AppendTimeoutException;
import org.menina.raft.exception.LeaderAwareException;
import org.menina.raft.exception.RaftException;
import org.menina.raft.exception.UnavailableException;
import org.menina.raft.message.RaftProto;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author zhenghao
 * @date 2019/2/12
 */
@Slf4j
public class Raft implements Proposer, Endpoint {

    private Node raftNode;

    private CountDownLatch available = new CountDownLatch(1);

    private Lock appendLock = new ReentrantLock();

    public Raft(Node raftNode) {
        Preconditions.checkNotNull(raftNode);
        raftNode.addGroupStateListener(new GroupStateListener() {
            @Override
            public void transition(State.GroupState from, State.GroupState to) {
                Preconditions.checkNotNull(to);
                if (to != State.GroupState.UNAVAILABLE || from == to) {
                    available.countDown();
                    available = new CountDownLatch(1);
                } else {
                    broadcastCommit();
                }
            }
        });

        this.raftNode = raftNode;
    }

    @Override
    public boolean isLeader() {
        return raftNode.isLeader();
    }

    @Override
    public NodeInfo leader() {
        return raftNode.leader();
    }

    @Override
    public boolean isReady() {
        return raftNode.nodeInfo().getReplayState().equals(State.ReplayState.REPLAYED);
    }

    @Override
    public long propose(byte[] data) throws RaftException {
        return propose(data, Constants.DEFAULT_APPEND_TIMEOUT_MILLS, TimeUnit.MILLISECONDS);
    }

    @Override
    public long propose(byte[] data, long timeout, TimeUnit timeUnit) throws RaftException {
        return propose(data, timeout, timeUnit, null);
    }

    @Override
    public long propose(byte[] data, long timeout, TimeUnit timeUnit, Map<String, String> attachments) throws RaftException {
        checkAvailable();
        long expectOffset;
        appendLock.lock();
        RaftProto.Entry.Builder entryBuilder = RaftProto.Entry.newBuilder()
                .setTerm(raftNode.currentTerm())
                .setType(RaftProto.EntryType.NORMAL)
                .setData(UnsafeByteOperations.unsafeWrap(data))
                .setCrc(RaftUtils.crc(data));

        if (attachments != null && attachments.size() > 0) {
            entryBuilder.putAllAttachments(attachments);
        }

        RaftProto.Message.Builder messageBuilder = RaftProto.Message.newBuilder()
                .setType(RaftProto.MessageType.PROPOSE)
                .setTerm(raftNode.currentTerm())
                .setFrom(raftNode.config().getId());

        try {
            long index = raftNode.next().incrementOffset();
            expectOffset = index;
            leader().getTransporter().request(messageBuilder.addEntries(entryBuilder.setIndex(index).build()).build());
        } catch (Exception e) {
            throw new RaftException(e.getMessage(), e);
        } finally {
            appendLock.unlock();
        }

        boolean completed = false;
        long deadline = timeUnit.toMillis(timeout) / raftNode.config().getClockAccuracyMills() + raftNode.clock().now();
        while (timeout == 0 || deadline > raftNode.clock().now()) {
            if (expectOffset <= raftNode.nodeInfo().getCommitted()) {
                completed = true;
                break;
            } else {
                checkAvailable();
                try {
                    ensureCommit();
                } catch (InterruptedException e) {
                    log.info(e.getMessage(), e);
                }
            }
        }

        if (!completed) {
            throw new AppendTimeoutException(data);
        }

        return expectOffset;
    }

    @Override
    public void start() {
        raftNode.start();
    }

    @Override
    public void close() {
        raftNode.close();
    }

    @Override
    public CompletableFuture closeFuture() {
        return raftNode.closeFuture();
    }

    public Node node() {
        return raftNode;
    }

    private void checkAvailable() throws LeaderAwareException, UnavailableException {
        if (!isLeader()) {
            NodeInfo leader = leader();
            if (leader == null) {
                throw new LeaderAwareException("Leader is not available");
            } else {
                throw new LeaderAwareException(leader.getId(), leader.getHost(), leader.getPort());
            }
        }

        if (raftNode.groupState().equals(State.GroupState.UNAVAILABLE)) {
            try {
                int remaining = raftNode.config().getRaftGroupUnavailableTimeoutMills();
                if (remaining == Integer.MAX_VALUE) {
                    log.warn("current cluster state {}, block thread {} until cluster state recover", raftNode.groupState(), Thread.currentThread());
                } else {
                    log.warn("current cluster state {}, thread {} max waiting {} seconds for cluster state recover", raftNode.groupState(), Thread.currentThread(), TimeUnit.MILLISECONDS.toSeconds(remaining));
                }

                if (!available.await(remaining, TimeUnit.MILLISECONDS) || node().groupState().equals(State.GroupState.UNAVAILABLE)) {
                    throw new UnavailableException("current cluster state is " + raftNode.groupState());
                }
            } catch (InterruptedException e) {
                throw new UnavailableException("current cluster state is " + raftNode.groupState());
            }
        }
    }

    private void ensureCommit() throws InterruptedException {
        raftNode.commitLock().lock();
        try {
            raftNode.commitSemaphore().await();
        } finally {
            raftNode.commitLock().unlock();
        }
    }

    private void broadcastCommit() {
        raftNode.commitLock().lock();
        try {
            raftNode.commitSemaphore().signalAll();
        } finally {
            raftNode.commitLock().unlock();
        }
    }
}
