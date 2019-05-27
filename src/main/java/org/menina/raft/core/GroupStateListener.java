package org.menina.raft.core;


import org.menina.raft.api.State;

/**
 * @author zhenghao
 * @date 2019/4/11
 */
public interface GroupStateListener {

    /**
     * group state transition
     * @param from
     * @param to
     */
    void transition(State.GroupState from, State.GroupState to);
}
