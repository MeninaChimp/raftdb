package org.menina.raft.common.task;

import com.google.common.base.Preconditions;
import org.menina.raft.api.Node;
import org.menina.raft.api.State;
import org.menina.raft.transport.RpcTransporter;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.TimeUnit;

/**
 * @author zhenghao
 * @date 2019/6/5
 */
@Slf4j
public class SentinelTask implements Runnable {

    private Node raftNode;

    public SentinelTask(Node raftNode) {
        Preconditions.checkNotNull(raftNode);
        this.raftNode = raftNode;
    }

    @Override
    public void run() {
        if (raftNode.groupState().equals(State.GroupState.UNAVAILABLE)
                || !raftNode.nodeInfo().getReplayState().equals(State.ReplayState.REPLAYED)) {
            return;
        }

        long now = raftNode.clock().now();
        long maxApplyMills = raftNode.config().getApplyThreadTimeoutSeconds();
        if (TimeUnit.MILLISECONDS.toSeconds((now - raftNode.nodeInfo().getApplyTick()) * raftNode.config().getClockAccuracyMills()) > maxApplyMills) {
            log.warn("apply thread blocked more than {} seconds, please check state machine processing status", maxApplyMills);
            raftNode.nodeInfo().setApplyTick(raftNode.clock().now());
        }

        long maxCommitMills = raftNode.config().getGroupCommitThreadTimeoutSeconds();
        if (TimeUnit.MILLISECONDS.toSeconds((now - raftNode.nodeInfo().getGroupCommitTick()) * raftNode.config().getClockAccuracyMills()) > maxCommitMills) {
            log.warn("group commit thread blocked more than {} seconds, please check network-io, disk-io, load, " +
                    "current node processing capacity is close to the upper limit", maxCommitMills);
            log.info("--------------node state-------------");
            log.info(raftNode.nodeInfo().toString());
            if (RpcTransporter.class.isAssignableFrom(raftNode.transporter().getClass())) {
                log.info(((RpcTransporter) (raftNode.transporter())).getRequestChannel().toString());
            }

            log.info("-------------node state end-------------");
            raftNode.nodeInfo().setGroupCommitTick(raftNode.clock().now());
        }
    }
}
