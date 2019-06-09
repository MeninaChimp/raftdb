package org.menina.raft.election;

import org.menina.raft.api.State;

/**
 * @author zhenghao
 * @date 2019/5/29
 */
public interface ElectionListener {

    /**
     * node role transfer
     * @param status
     */
    void transferTo(State.Status status);
}
