package org.menina.raft.api;

/**
 * @author zhenghao
 * @date 2019/1/23
 */
public interface State {

    /**
     * wal replay state
     */
    public enum ReplayState{

        /**
         * load snapshot and wal replay finished
         */
        REPLAYED,

        /**
         * loading snapshot and replaying wal
         */
        REPLAYING,

        /**
         * replay log has not started yet
         */
        UNREPLAY;
    }

    /**
     * raft group state
     */
    public enum GroupState {

        /**
         * all node are available
         */
        STABLE,

        /**
         * more than half node are available
         */
        PARTIAL,

        /**
         * less than half node are available
         */
        UNAVAILABLE
    }

    /**
     * node role
     */
    public enum Status {

        /**
         * node role is leader
         */
        LEADER,

        /**
         * node role is pre-candidate
         */
        PRECANDIDATE,

        /**
         * node role is candidate
         */
        CANDIDATE,

        /**
         * node role is follower
         */
        FOLLOWER
    }

    void becomeLeader();

    void becomeCandidate();

    void becomePreCandidate();

    void becomeFollower(long term, Integer leader);
}
