package org.menina.raft;

import com.google.common.base.Preconditions;
import org.menina.raft.api.State;
import org.menina.raft.common.RaftUtils;
import org.menina.raft.core.RaftNode;
import org.menina.raft.election.ElectionListener;
import org.menina.raft.mock.MockStateMachine;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

/**
 * @author zhenghao
 * @date 2019/2/11
 */
@Slf4j
public class RaftDistributeNodeTest {

    private static final int appendTaskNum = 1;
    private static CyclicBarrier barrier = new CyclicBarrier(appendTaskNum);
    private static AtomicBoolean report = new AtomicBoolean(false);
    private static Executor executor = Executors.newFixedThreadPool(appendTaskNum);
    private static MockStateMachine mockStateMachine = new MockStateMachine();

    public static void main(String[] args) throws IOException {
        RaftNode raftNode = new RaftNode(RaftUtils.extractConfigFromYml(), mockStateMachine);
        Raft raft = new Raft(raftNode);
        raft.start();
        try {
            for (int i = 0; i < appendTaskNum; i++) {
                executor.execute(new AppendTask(raft));
            }

            raftNode.closeFuture().get();
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    @Slf4j
    static class AppendTask implements Runnable {

        private Raft raft;

        AppendTask(Raft raft) {
            Preconditions.checkNotNull(raft);
            this.raft = raft;
        }

        @Override
        public void run() {
            raft.node().addElectionListener(new ElectionListener() {
                @Override
                public void transferTo(State.Status status) {
                    if (status.equals(State.Status.LEADER)) {
                        log.info("current node {} become leader", raft.node().config().getId());
                        mockStateMachine.setLeader(true);
                    }
                }
            });

            ByteBuffer buffer = ByteBuffer.allocate(512);
            for (Long i = 1L; i <= 64L; i++) {
                buffer.putLong(i);
            }

            int capacity = 10000;
            long allBegin;
            try {
                while (true) {
                    if (raft.isLeader()) {
                        if (raft.isReady()) {
                            log.info("propose start");
                            allBegin = System.currentTimeMillis();
                            for (int i = 0; i < capacity; i++) {
                                raft.propose(buffer.array());
                            }

                            barrier.await();
                            break;
                        } else {
                            log.info("leader not ready");
                            LockSupport.parkNanos(1000);
                        }
                    } else {
                        LockSupport.parkNanos(5000 * 1000 * 1000L);
                        mockStateMachine.setLeader(false);
                    }
                }

                if (report.compareAndSet(false, true)) {
                    long cost = System.currentTimeMillis() - allBegin;
                    log.info("total append time cost {} ms", cost);
                    log.info("append rate {}/s", appendTaskNum * capacity * 1000 / cost);
                }

                LockSupport.park();
            } catch (
                    Exception e)

            {
                log.error(e.getMessage(), e);
            }

        }
    }
}

