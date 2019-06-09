package org.menina.raft.core;

import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;
import com.google.protobuf.UnsafeByteOperations;
import org.menina.raft.api.Node;
import org.menina.raft.api.RaftApis;
import org.menina.raft.api.State;
import org.menina.raft.common.Constants;
import org.menina.raft.common.HardState;
import org.menina.raft.common.NodeInfo;
import org.menina.raft.common.Ready;
import org.menina.raft.election.CampaignType;
import org.menina.raft.message.RaftProto;
import org.menina.raft.log.Log;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * @author zhenghao
 * @date 2019/2/11
 */
@Slf4j
@NotThreadSafe
public class DefaultRaftApis extends AbstractMailbox implements RaftApis {

    private Node raftNode;

    private Log raftLog;

    private HardState hardState = new HardState();

    private Ready ready = new Ready();

    public DefaultRaftApis(Node raftNode) {
        Preconditions.checkNotNull(raftNode);
        this.raftNode = raftNode;
        this.raftLog = raftNode.raftLog();
    }

    @Override
    public void handleEvent(RaftProto.Message message) {
        if (raftNode.groupState().equals(State.GroupState.UNAVAILABLE)
                && (message.getType().equals(RaftProto.MessageType.HUP)
                || message.getType().equals(RaftProto.MessageType.HEART_BROADCAST))) {
            log.info("cluster state {}", raftNode.groupState());
            return;
        }

        if (message.getTerm() > raftNode.currentTerm()) {
            handleHighTermEvent(message);
        }

        if (message.getTerm() < raftNode.currentTerm()) {
            handleLowTermEvent(message);
            log.debug("ignore process {} from {} for low term", message.getType(), message.getFrom());
            return;
        }

        switch (message.getType()) {
            case NOP:
                break;
            case HEARTBEAT:
                handleHeartbeatEvent(message);
                break;
            case HEARTBEAT_RESPONSE:
                handleHeartbeatResponseEvent(message);
                break;
            case HUP:
                handleHupEvent(message);
                break;
            case HEART_BROADCAST:
                broadcastHeartbeat();
                break;
            case ENTRY_BROADCAST:
                broadcastAppend();
                break;
            case PREVOTE:
            case VOTE:
                handleVoteEvent(message);
                break;
            case SNAPSHOT_REQUEST:
                handleSnapshotEvent(message);
                break;
            case SNAPSHOT_RESPONSE:
                handleSnapshotResponse(message);
                break;
            case APPEND_ENTRIES_REQUEST:
                handleAppendEvent(message);
                break;
            case PROPOSE:
                handleProposeEvent(message);
                break;
            case VOTE_RESPONSE:
                handleVoteResponse(message);
                break;
            case APPEND_ENTRIES_RESPONSE:
                handleAppendEntriesResponse(message);
                break;
            default:
        }
    }

    @Override
    public void campaign(CampaignType type) {
        Preconditions.checkNotNull(type);
        boolean preVote = type.equals(CampaignType.PreElection);
        if (preVote) {
            log.info("node {} start pre election", this.raftNode.config().getId());
            raftNode.becomePreCandidate();
        } else {
            log.info("node {} start election", this.raftNode.config().getId());
            raftNode.becomeCandidate();
        }

        Long lastLogIndex = raftLog.lastIndex();
        RaftProto.Message.Builder voteRequest = RaftProto.Message.newBuilder()
                .setTerm(raftNode.currentTerm())
                .setType(preVote ? RaftProto.MessageType.PREVOTE : RaftProto.MessageType.VOTE)
                .setFrom(this.raftNode.config().getId())
                .setLogTerm(raftLog.term(lastLogIndex))
                .setIndex(lastLogIndex);

        raftNode.peers().values().iterator().forEachRemaining(new Consumer<NodeInfo>() {
            @Override
            public void accept(NodeInfo nodeInfo) {
                send(voteRequest.setTo(nodeInfo.getId()).build());
                log.info("proposer {} send {} to node {} success", raftNode.config().getId(), voteRequest.getType(), nodeInfo.getId());
            }
        });
    }

    @Override
    public Ready newReady() {
        hardState.setCommitted(raftNode.nodeInfo().getCommitted());
        hardState.setTerm(raftNode.currentTerm());
        hardState.setVoteFor(raftNode.voteFor());
        List<RaftProto.Entry> commit = raftLog.nextCommittedEntries(Constants.MAX_ENTRIES_LENGTH_LIMIT);
        List<RaftProto.Message> messages = this.drain();
        List<RaftProto.Entry> entries = raftLog.unstableLogs();
        ready.setEntries(entries);
        ready.setCommittedEntries(commit);
        ready.setMessages(messages);
        ready.setState(hardState);
        ready.setSnapshot(raftLog.snapshot());
        return ready;
    }

    @Override
    public void triggerToSnapshot() {
        if (raftLog.applied() - raftLog.firstIndex() < raftNode.config().getMaxSnapshotLagSize() || raftNode.nodeInfo().isSnapshot()) {
            return;
        }

        raftNode.nodeInfo().setSnapshot(true);
        try {
            long begin = raftNode.clock().now();
            log.info("start build snapshot for state machine");
            ByteBuffer buffer = raftNode.stateMachine().buildsSnapshot();
            if (buffer.isReadOnly()) {
                log.warn("snapshot is readOnly");
            }

            if (buffer.limit() == 0) {
                log.info("interrupt save snapshot for empty data");
                return;
            }

            long mills = (raftNode.clock().now() - begin) * raftNode.config().getClockAccuracyMills();
            RaftProto.SnapshotMetadata metadata = RaftProto.SnapshotMetadata
                    .newBuilder()
                    .setIndex(raftLog.applied())
                    .setTerm(raftLog.term(raftLog.applied()))
                    .build();
            RaftProto.Snapshot snapshot = RaftProto.Snapshot
                    .newBuilder()
                    .setData(buffer.isReadOnly()
                            ? ByteString.copyFrom(buffer)
                            : UnsafeByteOperations.unsafeWrap(buffer))
                    .setMeta(metadata)
                    .build();
            log.info("build snapshot from state machine, metadata index {}, term {}, size {} KB, time cost {} ms", metadata.getIndex(), metadata.getTerm(), buffer.limit() >> 10, mills);
            raftNode.snapshotter().save(snapshot);
        } catch (Throwable t) {
            log.error(t.getMessage(), t);
        } finally {
            raftNode.nodeInfo().setSnapshot(false);
        }
    }

    private void handleHighTermEvent(RaftProto.Message message) {
        Preconditions.checkNotNull(message);
        switch (message.getType()) {
            case PREVOTE:
                // pre vote should not disturb term
                break;
            case SNAPSHOT_REQUEST:
            case APPEND_ENTRIES_REQUEST:
            case HEARTBEAT:
                raftNode.becomeFollower(message.getTerm(), message.getFrom());
                break;
            default:
                log.info("node {} receive higher term from node {}, node status step down, term {} -> {}", raftNode.config().getId(), message.getFrom(), raftNode.currentTerm(), message.getTerm());
                raftNode.becomeFollower(message.getTerm(), null);
        }
    }

    private void handleLowTermEvent(RaftProto.Message message) {
        RaftProto.Message.Builder builder = RaftProto.Message.newBuilder()
                .setTerm(raftNode.currentTerm())
                .setFrom(raftNode.config().getId())
                .setTo(message.getFrom())
                .setReject(true)
                .setRejectType(RaftProto.RejectType.LOW_TERM);
        switch (message.getType()) {
            case PREVOTE:
            case VOTE:
                builder.setType(RaftProto.MessageType.VOTE_RESPONSE);
                break;
            case HEARTBEAT:
            case APPEND_ENTRIES_REQUEST:
                builder.setType(RaftProto.MessageType.APPEND_ENTRIES_RESPONSE);
                break;
            default:
                builder.setType(RaftProto.MessageType.NOP);
        }

        send(builder.build());
    }

    private void handleHeartbeatEvent(RaftProto.Message message) {
        // the heartbeat should guarantee that the committed value will not be greater than
        // the match index cached on the leader.
        log.debug("node {} receive heartbeat from leader {}, commit index {}",
                this.raftNode.config().getId(),
                message.getFrom(),
                message.getCommitIndex());
        raftNode.becomeFollower(message.getTerm(), message.getFrom());
        if (message.getCommitIndex() > raftNode.nodeInfo().getCommitted()) {
            raftNode.nodeInfo().setCommitted(message.getCommitIndex());
            log.debug("new commit index {} from leader {} by {}, update node {} commit index to {}",
                    message.getCommitIndex(),
                    message.getFrom(),
                    message.getType(),
                    raftNode.nodeInfo().getId(),
                    message.getCommitIndex());
        }

        send(RaftProto.Message.newBuilder()
                .setTerm(raftNode.currentTerm())
                .setFrom(raftNode.config().getId())
                .setTo(message.getFrom())
                .setCommitIndex(raftNode.nodeInfo().getCommitted())
                .setType(RaftProto.MessageType.HEARTBEAT_RESPONSE)
                .build());
    }

    private void handleHeartbeatResponseEvent(RaftProto.Message message) {
        raftNode.peers().get(message.getFrom()).setCommitted(message.getCommitIndex());
    }

    private void handleSnapshotEvent(RaftProto.Message message) {
        raftNode.becomeFollower(message.getTerm(), message.getFrom());
        RaftProto.SnapshotMetadata metadata = message.getSnapshot().getMeta();
        RaftProto.Message.Builder response = RaftProto.Message.newBuilder()
                .setTerm(raftNode.currentTerm())
                .setFrom(raftNode.config().getId())
                .setTo(message.getFrom())
                .setType(RaftProto.MessageType.SNAPSHOT_RESPONSE);
        long committed = raftNode.nodeInfo().getCommitted();
        // log align cause we should use committed instead of lastIndex
        if (metadata.getIndex() <= committed) {
            log.info("snapshot index {}, committed index {}, not allow apply the snapshot", metadata.getIndex(), committed);
            send(response.setIndex(committed)
                    .setReject(true)
                    .build());
        } else if (raftLog.matchTerm(metadata.getIndex(), metadata.getTerm())) {
            log.info("found match entry for snapshot index {}, term {}, ignore snapshot", metadata.getIndex(), metadata.getTerm());
            send(response.setIndex(raftLog.lastIndex())
                    .setReject(true)
                    .build());
        } else {
            log.info("install snapshot from leader {}", message.getFrom());
            raftLog.submitSnapshot(message.getSnapshot());
            send(response.setIndex(raftLog.firstIndex()).build());
        }
    }

    private void handleAppendEvent(RaftProto.Message message) {
        raftNode.becomeFollower(message.getTerm(), message.getFrom());
        long index = raftLog.checkPrefixEntry(message.getIndex(), message.getLogTerm());
        RaftProto.Message.Builder builder = RaftProto.Message.newBuilder()
                .setReject(true)
                .setRejectType(RaftProto.RejectType.LOG_NOT_MATCH)
                .setTerm(raftNode.currentTerm())
                .setFrom(raftNode.config().getId())
                .setTo(message.getFrom())
                .setIndex(index)
                .setType(RaftProto.MessageType.APPEND_ENTRIES_RESPONSE);
        if (index != message.getIndex()) {
            send(builder.build());
        } else {
            if (raftLog.lastIndex() > index && index != Constants.DEFAULT_INIT_OFFSET) {
                log.info("Found log conflict starting point, truncate current node {} log from index {} (included)",
                        raftNode.config().getId(),
                        index + 1);

                raftLog.truncate(index + 1);
            }

            int appends = 0;
            for (RaftProto.Entry entry : message.getEntriesList()) {
                index++;
                if (entry.getIndex() == index) {
                    appends++;
                } else {
                    log.info("detect non-sequential append entries, next index should be {}, but given entry index is {}", index, entry.getIndex());
                    index--;
                    break;
                }
            }

            if (appends > 0) {
                raftLog.append(message.getEntriesList().subList(0, appends));
                raftNode.nodeInfo(message.getFrom()).setCommitted(message.getCommitIndex());
                long commitIndex = Math.min(raftLog.lastIndex(), message.getCommitIndex());
                raftNode.nodeInfo().setCommitted(commitIndex);
                log.debug("new commit index {} from leader {} by {}, update node {} commit index to {}",
                        message.getCommitIndex(),
                        message.getFrom(),
                        message.getType(),
                        raftNode.nodeInfo().getId(),
                        commitIndex);
            }

            send(builder.setReject(false)
                    .setIndex(index)
                    .setCommitIndex(raftNode.nodeInfo(message.getFrom()).getCommitted())
                    .build());
        }
    }

    private void handleHupEvent(RaftProto.Message message) {
        Preconditions.checkArgument(message.getFrom() == raftNode.config().getId());
        if (raftNode.config().isPreVote()) {
            this.campaign(CampaignType.PreElection);
        } else {
            this.campaign(CampaignType.Election);
        }
    }

    private void handleVoteEvent(RaftProto.Message message) {
        RaftProto.Message.Builder builder = RaftProto.Message.newBuilder()
                .setTerm(raftNode.currentTerm())
                .setType(RaftProto.MessageType.VOTE_RESPONSE)
                .setFrom(this.raftNode.config().getId())
                .setTo(message.getFrom())
                .setReject(false);
        boolean canVote = raftNode.voteFor() == message.getFrom()
                || (raftNode.voteFor() == Constants.NOT_VOTE) && raftNode.leader() == null
                // avoid request node never change state from pre-candidate to candidate
                || (message.getType().equals(RaftProto.MessageType.PREVOTE) && message.getTerm() > raftNode.currentTerm());
        if (canVote && raftLog.isMoreUpToDate(message.getLogTerm(), message.getIndex())) {
            // pre vote not record voteFor, cause proposer will not mark vote self too at preVote step.
            if (RaftProto.MessageType.VOTE.equals(message.getType())) {
                log.debug("vote for node {}", message.getFrom());
                raftNode.vote(message.getFrom());
            }

            send(builder.build());
            log.debug("accept {} request from node {}, send response success to node {}", message.getType(), message.getFrom(), builder.getTo());
        } else {
            send(builder.setReject(true)
                    .setRejectType(RaftProto.RejectType.LOG_NOT_MATCH)
                    .build());
            log.debug("reject {} request from node {}, vote for {}, send response success to node {}", message.getType(), message.getFrom(), raftNode.voteFor(), builder.getTo());
        }
    }

    private void broadcastHeartbeat() {
        raftNode.peers().values().iterator().forEachRemaining(new Consumer<NodeInfo>() {
            @Override
            public void accept(NodeInfo nodeInfo) {
                send(RaftProto.Message.newBuilder()
                        .setType(RaftProto.MessageType.HEARTBEAT)
                        .setTerm(raftNode.currentTerm())
                        .setFrom(raftNode.config().getId())
                        .setTo(nodeInfo.getId())
                        // restricting the commit index carried by the heartbeat needs to take effect after the log alignment is
                        // completed, with matchIndex compare is necessary for old leader crashes and rejoins the cluster
                        .setCommitIndex(Math.min(nodeInfo.getMatchIndex(), raftNode.nodeInfo().getCommitted()))
                        .build());
                log.debug("broadcast heartbeat to {} completed", nodeInfo);
            }
        });
    }

    private void handleProposeEvent(RaftProto.Message message) {
        raftLog.append(message.getEntriesList());
        long matchIndex = this.raftLog.lastIndex();
        raftNode.nodeInfo().setMatchIndex(matchIndex);
        raftNode.nodeInfo().setNextIndex(matchIndex + 1);
        broadcastAppend();
    }

    private void broadcastAppend() {
        raftNode.peers().values().iterator().forEachRemaining(new Consumer<NodeInfo>() {
            @Override
            public void accept(NodeInfo nodeInfo) {
                if (!nodeInfo.isPaused() && !nodeInfo.isTransportSnapshot()) {
                    RaftProto.Message request;
                    if (raftLog.maySendSnapshot(nodeInfo.getNextIndex())) {
                        try {
                            RaftProto.Snapshot snapshot = raftNode.snapshotter().loadNewest();
                            log.info("need send snapshot {} to node {}", snapshot.getMeta().getIndex(), nodeInfo.getId());
                            request = RaftProto.Message.newBuilder()
                                    .setTerm(raftNode.currentTerm())
                                    .setType(RaftProto.MessageType.SNAPSHOT_REQUEST)
                                    .setFrom(raftNode.config().getId())
                                    .setTo(nodeInfo.getId())
                                    .setSnapshot(snapshot)
                                    .build();
                            nodeInfo.setTransportSnapshot(true);
                        } catch (IOException e) {
                            throw new IllegalStateException(e.getMessage(), e);
                        }
                    } else {
                        long preLogIndex = nodeInfo.getNextIndex() - 1;
                        long preLogTerm;
                        if (preLogIndex == raftNode.raftLog().firstIndex()) {
                            preLogTerm = raftNode.raftLog().firstTerm();
                        } else {
                            RaftProto.Entry entry = raftLog.entry(preLogIndex);
                            if (entry == null) {
                                log.info("offset is not first stabled, delay send entries to node {}", nodeInfo.getId());
                                return;
                            }

                            preLogTerm = entry.getTerm();
                        }

                        List<RaftProto.Entry> entries = raftLog.entries(nodeInfo.getNextIndex(), Constants.MAX_ENTRIES_LENGTH_LIMIT);
                        if (entries.size() > 0) {
                            if (entries.get(0).getIndex() != nodeInfo.getNextIndex()) {
                                log.error("illegal fetch entries from leader log, fetch index {}, entries start offset {}",
                                        nodeInfo.getNextIndex(),
                                        entries.get(0).getIndex());
                            }
                        }

                        request = RaftProto.Message.newBuilder()
                                .setTerm(raftNode.currentTerm())
                                .setType(RaftProto.MessageType.APPEND_ENTRIES_REQUEST)
                                .addAllEntries(entries)
                                .setIndex(preLogIndex)
                                .setLogTerm(preLogTerm)
                                .setFrom(raftNode.config().getId())
                                .setTo(nodeInfo.getId())
                                .setCommitIndex(raftNode.nodeInfo().getCommitted())
                                .build();
                    }

                    log.debug("turn on pause for node {}", nodeInfo.getId());
                    nodeInfo.setPaused(true);
                    send(request);
                }
            }
        });
    }

    private void handleVoteResponse(RaftProto.Message message) {
        log.debug("node {} receive vote response from {} for grant {}", raftNode.config().getId(), message.getFrom(), !message.getReject());
        Map<Integer, Boolean> votes = raftNode.votes();
        // vote self
        votes.put(raftNode.config().getId(), true);
        final int[] granted = {0};
        if (!votes.containsKey(message.getFrom())) {
            votes.put(message.getFrom(), !message.getReject());
        }

        votes.values().iterator().forEachRemaining(new Consumer<Boolean>() {
            @Override
            public void accept(Boolean vote) {
                if (vote) {
                    granted[0] += 1;
                }
            }
        });

        if (granted[0] == raftNode.quorum()) {
            log.debug("node {} { state {} } receive enough vote from others, granted: {}", this.raftNode.config().getId(), this.raftNode.status(), granted[0]);
            switch (raftNode.status()) {
                // maybe need distinguish pre-vote response and vote response
                case PRECANDIDATE:
                    this.campaign(CampaignType.Election);
                    break;
                case CANDIDATE:
                    this.raftNode.becomeLeader();
                    this.broadcastAppend();
                    break;
                case LEADER:
                    // ignore
                    break;
                default:
                    log.error("illegal vote state for node {}, current state {}", this.raftNode.config().getId(), this.raftNode.status());
                    throw new IllegalArgumentException();
            }
        } else if (raftNode.votes().size() - granted[0] == raftNode.quorum()) {
            log.debug("node {} propose failed, step down", this.raftNode.config().getId());
            raftNode.becomeFollower(raftNode.currentTerm(), null);
        }
    }

    private void handleSnapshotResponse(RaftProto.Message message) {
        NodeInfo peer = raftNode.nodeInfo(message.getFrom());
        if (!message.getReject()) {
            peer.setMatchIndex(message.getIndex());
            peer.setNextIndex(message.getIndex() + 1);
        }

        peer.setTransportSnapshot(false);
        peer.setPaused(false);
    }

    private void handleAppendEntriesResponse(RaftProto.Message message) {
        NodeInfo peer = raftNode.nodeInfo(message.getFrom());
        long lastNextIndex = peer.getNextIndex();
        peer.setMatchIndex(message.getIndex());
        peer.setNextIndex(message.getIndex() + 1);
        peer.setPaused(false);
        if (lastNextIndex > peer.getNextIndex()) {
            log.info("log alignment occurs, we need an additional broadcast to node {} make the node writable, old next index {}, new next index {}",
                    peer.getId(), lastNextIndex, peer.getNextIndex());
            broadcastAppend();
        }

        log.debug("turn off pause for node {}, next index {}", peer.getId(), message.getIndex() + 1);
        if (!message.getReject()) {
            int nodes = raftNode.cluster().size();
            long[] matchIndexes = new long[nodes];
            final int[] index = {0};
            raftNode.peers().values().iterator().forEachRemaining(new Consumer<NodeInfo>() {
                @Override
                public void accept(NodeInfo nodeInfo) {
                    matchIndexes[index[0]] = nodeInfo.getMatchIndex();
                    index[0]++;
                }
            });

            matchIndexes[index[0]++] = raftLog.lastIndex();
            Arrays.sort(matchIndexes);
            long newCommittedIndex = matchIndexes[nodes / 2];
            if (raftNode.nodeInfo().getCommitted() < newCommittedIndex) {
                raftNode.nodeInfo().setCommitted(newCommittedIndex);
                raftNode.commitLock().lock();
                try {
                    raftNode.commitSemaphore().signalAll();
                } finally {
                    raftNode.commitLock().unlock();
                }
            }
        }
    }
}
