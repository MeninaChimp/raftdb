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
     *
     * @return
     */
    boolean isLeader();

    /**
     * leader aware
     *
     * @return
     */
    NodeInfo leader();

    /**
     * if true, indicates that the wal has been applied to state machine, if current node is leader,
     * so the cluster is ready to accept write requests, this method ensures that the status is
     * consistent after the leader switchover
     *
     * @return
     */
    boolean isReady();

    /**
     * submit proposal to raft protocol with block
     *
     * @param data
     * @return
     * @throws RaftException
     */
    long propose(byte[] data) throws RaftException;

    /**
     * submit proposal to raft protocol with timeout, zero timeout will block current thread
     * until the message was submitted successfully
     *
     * @param data
     * @param timeout
     * @param timeUnit
     * @return
     * @throws RaftException
     */
    long propose(byte[] data, long timeout, TimeUnit timeUnit) throws RaftException;

    /**
     * submit proposal to raft protocol with timeout and attachments, zero timeout will block
     * current thread until the message was submitted successfully
     *
     * @param data
     * @param timeout
     * @param timeUnit
     * @param attachments
     * @return
     * @throws RaftException
     */
    long propose(byte[] data, long timeout, TimeUnit timeUnit, Map<String, String> attachments) throws RaftException;
}
