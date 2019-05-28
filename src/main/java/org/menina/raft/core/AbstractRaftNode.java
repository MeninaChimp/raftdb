package org.menina.raft.core;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.menina.raft.api.Node;
import org.menina.raft.common.Constants;
import org.menina.raft.common.NodeInfo;
import org.menina.raft.common.RaftConfig;
import org.menina.raft.common.RaftUtils;
import org.menina.raft.common.meta.NextOffsetMetaData;
import org.menina.raft.election.ElectionTick;
import org.menina.raft.election.HeartBeatTick;
import org.menina.raft.election.Tick;
import org.menina.raft.election.TickListener;
import org.menina.raft.log.Log;
import org.menina.raft.log.RaftLog;
import org.menina.raft.message.RaftProto;
import org.menina.raft.snapshot.DefaultSnapshotter;
import org.menina.raft.snapshot.Snapshotter;
import org.menina.raft.statemachine.StateMachine;
import org.menina.raft.storage.MemoryStorage;
import org.menina.raft.storage.PersistentStorage;
import org.menina.raft.storage.Storage;
import org.menina.raft.transport.RpcTransporter;
import org.menina.raft.transport.Transporter;
import org.menina.raft.wal.Wal;
import org.menina.rail.common.NamedThreadFactory;
import org.menina.rail.server.ExporterServer;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

/**
 * @author zhenghao
 * @date 2019/1/23
 */
@Slf4j
@NotThreadSafe
public abstract class AbstractRaftNode implements Node {

    protected Lock commitLock = new ReentrantLock();

    protected Condition commitSemaphore = commitLock.newCondition();

    protected RaftConfig config;

    protected long term = 0L;

    protected Integer leader;

    protected Status status;

    protected Tick clock;

    protected ExporterServer server;

    protected int voteFor = Constants.NOT_VOTE;

    protected ImmutableMap<Integer, NodeInfo> cluster;

    protected ImmutableMap<Integer, NodeInfo> peers;

    protected Transporter transporter;

    protected RequestChannel requestChannel;

    protected Storage storage;

    protected Wal wal;

    protected Log raftLog;

    protected Snapshotter snapshotter;

    protected StateMachine stateMachine;

    protected NextOffsetMetaData nextOffsetMetaData;

    protected ScheduledExecutorService backgroundExecutor;

    protected volatile GroupState groupState = GroupState.UNAVAILABLE;

    protected Map<Integer, Boolean> votes = Maps.newHashMap();

    protected List<GroupStateListener> groupStateListeners = Lists.newArrayList();

    protected TickListener electionTick = new ElectionTick(this);

    protected TickListener heartbeatTick = new HeartBeatTick(this);

    public AbstractRaftNode(RaftConfig config, StateMachine stateMachine) {
        checkArguments(config, stateMachine);
        ImmutableMap.Builder<Integer, NodeInfo> builder = new ImmutableMap.Builder<Integer, NodeInfo>();
        Set<Integer> ids = new HashSet<>();
        for (String address : config.getCluster().split(Constants.ADDRESS_SEPARATOR)) {
            NodeInfo nodeInfo = RaftUtils.parseAddress(address);
            int nodeId = nodeInfo.getId();
            Preconditions.checkArgument(nodeId > 0, "node id should be positive");
            if (ids.contains(nodeId)) {
                throw new IllegalStateException("unique id required, duplicate node id: " + nodeId);
            }

            builder.put(nodeId, nodeInfo);
            ids.add(nodeId);
        }

        this.cluster = builder.build();
        this.config = config;
        this.stateMachine = stateMachine;
        Map<Integer, NodeInfo> peers = Maps.newHashMap(this.cluster);
        peers.remove(this.config.getId());
        this.peers = ImmutableMap.copyOf(peers);
        this.requestChannel = new RequestChannel();
        this.transporter = new RpcTransporter(requestChannel);
        this.snapshotter = new DefaultSnapshotter(this);
        this.wal = new Wal(config);
        log.info("use storage type {}", config.getStorageType());
        switch (config.getStorageType()) {
            case DISK:
                this.storage = new PersistentStorage(wal);
                break;
            case MEMORY:
                this.storage = new MemoryStorage(wal);
                break;
            case COMBINATION:
                throw new UnsupportedOperationException();
            default:
                throw new UnsupportedOperationException();
        }

        this.raftLog = new RaftLog(this);
        this.backgroundExecutor = new ScheduledThreadPoolExecutor(config.getBackgroundThreadsNum(), new NamedThreadFactory(Constants.DEFAULT_SCHEDULE_BACKGROUND_THREAD));
    }

    @Override
    public Tick clock() {
        return clock;
    }

    @Override
    public StateMachine stateMachine() {
        return this.stateMachine;
    }

    @Override
    public Wal wal() {
        return this.wal;
    }

    @Override
    public Transporter transporter() {
        return this.transporter;
    }

    @Override
    public Storage storage() {
        return this.storage;
    }

    @Override
    public Log raftLog() {
        return raftLog;
    }

    @Override
    public Snapshotter snapshotter() {
        return snapshotter;
    }

    @Override
    public long currentTerm() {
        return this.term;
    }

    @Override
    public NodeInfo leader() {
        return this.leader == null ? null : cluster.get(this.leader);
    }

    @Override
    public boolean isLeader() {
        return leader() != null && leader().getId() == config.getId();
    }

    @Override
    public int voteFor() {
        return this.voteFor;
    }

    @Override
    public void vote(Integer node) {
        Preconditions.checkNotNull(node);
        this.voteFor = node;
    }

    @Override
    public Status status() {
        return status;
    }

    @Override
    public RaftConfig config() {
        return config;
    }

    @Override
    public Map<Integer, NodeInfo> cluster() {
        return cluster;
    }

    @Override
    public Map<Integer, NodeInfo> peers() {
        return peers;
    }

    @Override
    public NodeInfo nodeInfo(int id) {
        return cluster.get(id);
    }

    @Override
    public NodeInfo nodeInfo() {
        return cluster.get(config.getId());
    }

    @Override
    public Map<Integer, Boolean> votes() {
        return this.votes;
    }

    @Override
    public NextOffsetMetaData next() {
        return nextOffsetMetaData;
    }

    @Override
    public GroupState groupState() {
        return groupState;
    }

    @Override
    public void mayRefreshState() {
        int available = 1;
        for (NodeInfo nodeInfo : peers.values()) {
            if (!nodeInfo.isDisconnected()) {
                available += 1;
            }
        }

        GroupState record = this.groupState;
        if (available == cluster.values().size()) {
            this.groupState = GroupState.STABLE;
        } else if (available >= quorum()) {
            this.groupState = GroupState.PARTIAL;
        } else {
            this.groupState = GroupState.UNAVAILABLE;
        }

        if (record != this.groupState) {
            groupStateListeners.iterator().forEachRemaining(new Consumer<GroupStateListener>() {
                @Override
                public void accept(GroupStateListener listener) {
                    try {
                        listener.transition(record, groupState);
                    } catch (Throwable t) {
                        log.error(t.getMessage(), t);
                    }
                }
            });
        }
    }

    @Override
    public void addGroupStateListener(GroupStateListener listener) {
        Preconditions.checkNotNull(listener);
        groupStateListeners.add(listener);
    }

    @Override
    public Condition commitSemaphore() {
        return commitSemaphore;
    }

    @Override
    public Lock commitLock() {
        return commitLock;
    }

    @Override
    public int quorum() {
        return (cluster().size() >> 1) + 1;
    }

    @Override
    public void recover(RaftProto.Snapshot snapshot, RaftProto.Entry latest) {
        if (snapshot != null) {
            term = snapshot.getMeta().getTerm();
        }

        if (latest != null) {
            term = latest.getTerm();
        }
    }

    @Override
    public void close() {
        this.server.close();
    }

    @Override
    public CompletableFuture closeFuture() {
        return this.server.closeFuture();
    }

    @Override
    public void becomeLeader() {
        if (!Status.LEADER.equals(this.status)) {
            log.info("node {} become leader, {}, cluster term {}", config.getId(), cluster.get(config.getId()), this.term);
            this.votes.clear();
            this.status = Status.LEADER;
            this.voteFor = Constants.NOT_VOTE;
            this.leader = config.getId();
            this.clock.removeListener(Constants.ELECTION_TICK);
            this.clock.addListener(this.heartbeatTick);
            this.nextOffsetMetaData = new NextOffsetMetaData(raftLog.lastIndex());
            log.info("initialization leader NextOffsetMetaData to last index, {}", this.nextOffsetMetaData);
            peers.values().iterator().forEachRemaining(new Consumer<NodeInfo>() {
                @Override
                public void accept(NodeInfo nodeInfo) {
                    nodeInfo.setNextIndex(nextOffsetMetaData.getOffset() + 1);
                    log.info("reset node {} next index to {}", nodeInfo.getId(), nodeInfo.getNextIndex());
                }
            });
        } else {
            log.warn("{} already become leader", this.cluster.get(this.config.getId()));
        }
    }

    @Override
    public void becomeCandidate() {
        this.votes.clear();
        this.term = term + 1;
        this.voteFor = this.config.getId();
        this.status = Status.CANDIDATE;
        this.electionTick.reset();
        log.info("node {} become candidate, current term {}", this.config.getId(), this.term);
    }

    @Override
    public void becomePreCandidate() {
        this.votes.clear();
        this.status = Status.PRECANDIDATE;
        this.electionTick.reset();
        this.leader = null;
        log.debug("node {} become pre candidate, term {}", this.config.getId(), this.term);
    }

    @Override
    public void becomeFollower(long term, Integer leader) {
        if (Status.LEADER.equals(this.status)) {
            this.clock.addListener(this.electionTick);
            this.clock.removeListener(Constants.HEARTBEAT_TICK);
        }

        this.votes.clear();
        this.voteFor = Constants.NOT_VOTE;
        this.status = Status.FOLLOWER;
        this.term = term;
        this.leader = leader;
        this.electionTick.reset();
        log.debug("node {} become follower, current term {}, leader {}", this.config.getId(), this.term, this.leader);
    }

    private void checkArguments(RaftConfig config, StateMachine stateMachine) {
        Preconditions.checkNotNull(config);
        Preconditions.checkNotNull(config.getId());
        Preconditions.checkArgument(config.getId() > 0, "id must be positive");
        Preconditions.checkNotNull(config.getCluster());
        Preconditions.checkNotNull(stateMachine, "state machine should not be null");
        Preconditions.checkArgument(config.getMinSnapshotsRetention() > 0, "min snapshots retention should above than 0");
    }
}
