package org.menina.raft.api;

import org.menina.raft.common.NodeInfo;
import org.menina.raft.exception.RaftException;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @author zhenghao
 * @date 2019/4/11
 */
public interface Proposer {

    /**
     * check current node is leader
     * @return
     */
    boolean isLeader();

    /**
     * leader aware
     * @return
     */
    NodeInfo leader();

    /**
     * submit proposal to raft protocol with block
     * @param data
     * @throws RaftException
     */
    void propose(byte[] data) throws RaftException;

    /**
     * submit proposal to raft protocol with timeout, zero timeout will block current thread
     * until the message was submitted successfully
     * @param data
     * @param timeout
     * @param timeUnit
     * @throws RaftException
     */
    void propose(byte[] data, long timeout, TimeUnit timeUnit) throws RaftException;

    /**
     * submit proposal to raft protocol with timeout and attachments, zero timeout will block
     * current thread until the message was submitted successfully
     *
     * @param data
     * @param timeout
     * @param timeUnit
     * @param attachments
     * @throws RaftException
     */
    void propose(byte[] data, long timeout, TimeUnit timeUnit, Map<String, String> attachments) throws RaftException;
}
