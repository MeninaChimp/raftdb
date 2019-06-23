package org.menina.raft.api;

import org.menina.raft.common.NodeInfo;
import org.menina.raft.common.RaftConfig;
import org.menina.raft.common.meta.NextOffsetMetaData;
import org.menina.raft.core.GroupStateListener;
import org.menina.raft.election.ElectionListener;
import org.menina.raft.election.Tick;
import org.menina.raft.log.Log;
import org.menina.raft.message.RaftProto;
import org.menina.raft.snapshot.Snapshotter;
import org.menina.raft.statemachine.StateMachine;
import org.menina.raft.storage.Storage;
import org.menina.raft.transport.Transporter;
import org.menina.raft.wal.Wal;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

/**
 * @author zhenghao
 * @date 2019/1/23
 */
public interface Node extends State, Endpoint {

    /**
     * logic clock
     * @return
     */
    Tick clock();

    /**
     * StateMachine
     * @return
     */
    StateMachine stateMachine();

    /**
     * WAL
     * @return
     */
    Wal wal();

    /**
     * transporter
     */
    Transporter transporter();

    /**
     * Storage
     * @return
     */
    Storage storage();

    /**
     * raft log
     * @return
     */
    Log raftLog();

    /**
     * snapshotter
     * @return
     */
    Snapshotter snapshotter();

    /**
     * term aware
     *
     * @return
     */
    long currentTerm();

    /**
     * leader info
     *
     * @return
     */
    NodeInfo leader();

    /**
     * check if current node is leader
     * @return
     */
    boolean isLeader();

    /**
     * vote info
     *
     * @return
     */
    int voteFor();

    /**
     * vote
     *
     * @param node
     */
    void vote(Integer node);

    /**
     * status
     *
     * @return
     */
    Status status();

    /**
     * config aware
     *
     * @return
     */
    RaftConfig config();

    /**
     * cluster info
     *
     * @return
     */
    Map<Integer, NodeInfo> cluster();

    /**
     * peers info
     *
     * @return
     */
    Map<Integer, NodeInfo> peers();

    /**
     * get node info by node id
     * @param id
     * @return
     */
    NodeInfo nodeInfo(int id);

    /**
     * current node info
     * @return
     */
    NodeInfo nodeInfo();

    /**
     * vote response state from others for one term
     * @return
     */
    Map<Integer, Boolean> votes();

    /**
     * lease ack response from peers
     * @return
     */
    Set<Integer> leased();

    /**
     * next offset meta for message income
     * @return
     */
    NextOffsetMetaData next();

    /**
     * state for this group
     * @return
     */
    GroupState groupState();

    /**
     * trigger to refresh state for this group, if force is true, will notify listeners
     * current group state again regardless of whether a change has occurred
     * @param force
     */
    void mayRefreshState(boolean force);

    /**
     * group state change listener
     * @param listener
     */
    void addGroupStateListener(GroupStateListener listener);

    /**
     * election state change listener
     * @param listener
     */
    void addElectionListener(ElectionListener listener);

    /**
     * commit index refresh semaphore
     * @return
     */
    Condition commitSemaphore();

    /**
     * commit lock
     * @return
     */
    Lock commitLock();

    /**
     * quorum value
     * @return
     */
    int quorum();

    /**
     * recover state for node
     *
     * @param snapshot
     * @param latest
     */
    void recover(RaftProto.Snapshot snapshot, RaftProto.Entry latest);

    /**
     * apply index hook, allow non-Raft internal thread modify apply index
     *
     * @param index
     * @return
     */
    boolean applied(long index);

}
