package org.menina.raft.election;

import com.google.common.base.Preconditions;
import org.menina.raft.api.Node;
import org.menina.raft.common.Constants;
import org.menina.raft.message.RaftProto;
import lombok.extern.slf4j.Slf4j;

/**
 * @author zhenghao
 * @date 2019/1/24
 */
@Slf4j
public class HeartBeatTick implements TickListener {

    private Node raftNode;

    public HeartBeatTick(Node raftNode) {
        Preconditions.checkNotNull(raftNode);
        this.raftNode = raftNode;
    }

    private volatile long heartbeatElapsed;

    @Override
    public String id() {
        return Constants.HEARTBEAT_TICK;
    }

    @Override
    public void onTick(TickEvent event) {
        heartbeatElapsed++;
        if (heartbeatElapsed > raftNode.config().getHeartbeatTimeoutTick()) {
            this.raftNode.transporter()
                    .request(RaftProto.Message.newBuilder()
                            .setType(RaftProto.MessageType.HEART_BROADCAST)
                            .setTerm(raftNode.currentTerm())
                            .setFrom(raftNode.config().getId())
                            .build());
            reset();
        }
    }

    @Override
    public void reset() {
        this.heartbeatElapsed = 0;
    }
}
