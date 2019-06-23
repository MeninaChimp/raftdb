package org.menina.raft.election;

import org.menina.raft.api.Node;
import org.menina.raft.common.Constants;
import org.menina.raft.message.RaftProto;

/**
 * @author zhenghao
 * @date 2019/6/4
 */
public class LeaseTick implements TickListener{

    private Node raftNode;

    public LeaseTick(Node raftNode) {
        this.raftNode = raftNode;
    }

    private volatile long leaseElapsed = 0;

    @Override
    public String id() {
        return Constants.LEASE_TICK;
    }

    @Override
    public void onTick(TickEvent event) {
        leaseElapsed++;
        if (leaseElapsed >= raftNode.config().getLeaseTimeoutTick()) {
            this.raftNode.transporter()
                    .request(RaftProto.Message.newBuilder()
                            .setType(RaftProto.MessageType.LEASE)
                            .setTerm(raftNode.currentTerm())
                            .setFrom(raftNode.config().getId())
                            .build());
            reset();
        }
    }

    @Override
    public void reset() {
        this.leaseElapsed = 0;
    }
}
