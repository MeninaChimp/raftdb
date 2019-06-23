package org.menina.raft.log;

import org.menina.raft.message.RaftProto;
import org.menina.raft.snapshot.SnapshotFuture;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * @author zhenghao
 * @date 2019/2/12
 */
public interface Log {

    /**
     * append entries
     *
     * @param entries
     */
    void append(List<RaftProto.Entry> entries);

    /**
     * return last offset of raft log
     *
     * @return
     */
    long lastIndex();

    /**
     * get target term by given offset
     *
     * @param index
     * @return
     */
    long term(long index);

    /**
     * check term is match on given index
     * @param index
     * @param term
     * @return
     */
    boolean matchTerm(long index, long term);

    /**
     * detect whether up-to-date log
     *
     * @param lastLogTerm
     * @param lastLogIndex
     * @return
     */
    boolean isMoreUpToDate(long lastLogTerm, long lastLogIndex);

    /**
     * get entry by given offset
     *
     * @param index
     * @return
     */
    RaftProto.Entry entry(long index);

    /**
     * fetch entries by given start offset
     *
     * @param startIndex
     * @param limitSize
     * @return
     */
    List<RaftProto.Entry> entries(long startIndex, long limitSize);

    /**
     * fetch next batch committed entries for apply to state machine
     *
     * @param limitSize
     * @return
     */
    List<RaftProto.Entry> nextCommittedEntries(int limitSize);

    /**
     * track committed index
     *
     * @return
     */
    long committed();

    /**
     * mark offset which already apply to state machine
     *
     * @return
     */
    long applied();

    /**
     * get entries which need to stable to storage
     *
     * @return
     */
    List<RaftProto.Entry> unstableLogs();

    /**
     * reset committed offset
     *
     * @param offset
     */
    void commitTo(long offset);

    /**
     * reset applied offset
     * @param offset
     * @return
     */
    boolean appliedTo(long offset);

    /**
     * check prefix entry match for index and term
     *
     * @param index prefix index
     * @param term  term of prefix entry
     * @return next index
     */
    long checkPrefixEntry(long index, long term);

    /**
     * check whether need send snapshot
     *
     * @param offset
     * @return
     */
    boolean maySendSnapshot(long offset);

    /**
     * mark lower than given offset already persist on disk
     *
     * @param offset
     */
    void stableTo(long offset);

    /**
     * truncate log start from given offset, the given offset will be included
     *
     * @param startOffset
     * @return
     */
    boolean truncate(long startOffset);

    /**
     * snapshot and log demarcation point
     * @return
     */
    long firstIndex();

    /**
     * term of snapshot and log demarcation point
     * @return
     */
    long firstTerm();

    /**
     * update snapshot meta data
     * @param metadata
     */
    void updateSnapshotMetadata(RaftProto.SnapshotMetadata metadata);

    /**
     * submit snapshot to log
     * @param snapshot
     */
    CompletableFuture<RaftProto.Snapshot> submitSnapshot(RaftProto.Snapshot snapshot);

    /**
     * get snapshot
     * @return
     */
    SnapshotFuture snapshot();
}
