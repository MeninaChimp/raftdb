package org.menina.raft;

import org.menina.raft.common.Constants;
import org.menina.raft.common.RaftConfig;
import org.menina.raft.core.RaftNode;
import org.menina.raft.mock.MockStateMachine;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;

@Slf4j
public class RaftNodeTest {

    private static final String cluster = "127.0.0.1:1011:1 127.0.0.1:1012:2 127.0.0.1:1013:3";
    private static Executor executor = Executors.newFixedThreadPool(3);
    private static Lock startLock = new ReentrantLock();

    public static void main(String[] args) {
        executor.execute(new Node(1));
        executor.execute(new Node(2));
        executor.execute(new Node(3));
        LockSupport.park();
    }

    public static class Node implements Runnable {

        private int id;

        Node(int id) {
            this.id = id;
        }

        @Override
        public void run() {
            RaftConfig config = RaftConfig
                    .builder()
                    .cluster(cluster)
                    .id(id)
                    .wal(Constants.DEFAULT_WAL_DIR + File.separator + id)
                    .snap(Constants.DEFAULT_SNAP_DIR + File.separator + id)
                    .build();

            RaftNode raftNode = new RaftNode(config, new MockStateMachine());
            startLock.lock();
            try {
                raftNode.start();
            } finally {
                startLock.unlock();
            }

            try {
                raftNode.closeFuture().get();
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }
    }
}


