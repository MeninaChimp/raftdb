package org.menina.raft.statemachine;

import org.menina.raft.message.RaftProto;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * @author zhenghao
 * @date 2019/2/25
 *
 * guarantee single thread call, thread safe
 */
public interface StateMachine {

    /**
     * log apply callback
     *
     * @param entries
     */
    void apply(List<RaftProto.Entry> entries);

    /**
     * upper layer application snapshot generation trigger interface
     *
     * @return
     */
    ByteBuffer buildsSnapshot();

    /**
     * snapshot load trigger interface
     *
     * @param data
     */
    void applySnapshot(ByteBuffer data);

    /**
     * allow state machine to intervene in snapshot trigger timing, By default, when the log
     * reaches the accumulation limit, the snapshot build is triggered, if return false, this
     * method will be triggered continuously until the snapshot is created successfully.
     *
     * @return
     */
    boolean allowBuildSnapshot();

}
