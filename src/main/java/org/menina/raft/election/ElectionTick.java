package org.menina.raft.election;

import com.google.common.base.Preconditions;
import org.menina.raft.api.Node;
import org.menina.raft.api.State;
import org.menina.raft.common.Constants;
import org.menina.raft.message.RaftProto;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ThreadLocalRandom;

/**
 * @author zhenghao
 * @date 2019/1/24
 * <p>
 * No need for concurrency, not thread safe.
 */
@Slf4j
public class ElectionTick implements TickListener {

    private Node raftNode;

    public ElectionTick(Node raftNode) {
        Preconditions.checkNotNull(raftNode);
        this.raftNode = raftNode;
    }

    private volatile long electionElapsed;

    @Override
    public String id() {
        return Constants.ELECTION_TICK;
    }

    @Override
    public void onTick(TickEvent event) {
        electionElapsed++;
        if (electionElapsed >= raftNode.config().getElectionTimeoutTick() + ThreadLocalRandom.current().nextLong(0, Constants.DEFAULT_RANDOM_ELECTION_TIMEOUT)) {
            log.debug("node {} election event trigger", raftNode.config().getId());
            if (raftNode.status().equals(State.Status.LEADER)) {
                log.warn("already become leader, ignore election event");
                return;
            }

            raftNode.transporter().request(RaftProto.Message.newBuilder()
                    .setTerm(raftNode.currentTerm())
                    .setType(RaftProto.MessageType.HUP)
                    .setFrom(raftNode.config().getId())
                    .build());

            reset();
        }
    }

    @Override
    public void reset() {
        this.electionElapsed = 0;
    }
}
