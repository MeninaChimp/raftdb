package org.menina.raft.storage;

import org.menina.raft.message.RaftProto;

import java.io.IOException;
import java.util.List;

/**
 * @author zhenghao
 * @date 2019/2/16
 */
public interface Storage {

    /**
     * append entries to storage
     * @param entries
     */
    void append(List<RaftProto.Entry> entries);

    /**
     * last index of storage
     * @return
     */
    long lastIndex();

    /**
     * get entry for given offset
     * @param index
     * @return
     */
    RaftProto.Entry entry(long index);

    /**
     * fetch data from storage, the end index entry will not be included
     *
     * @param begin
     * @param end
     * @return
     */
    List<RaftProto.Entry> entries(long begin, long end);

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
     * reset snapshot meta data
     * @param metadata
     */
    void updateSnapshotMetadata(RaftProto.SnapshotMetadata metadata);

    /**
     * truncate log start from given offset, the given offset will be included
     *
     * @param startOffset
     * @return
     * @throws IOException
     */
    void truncateSuffix(long startOffset) throws IOException;

    /**
     * truncate log start from 0 to endOffset, the given offset will be included
     * @param endOffset
     * @return
     */
    void truncatePrefix(long endOffset);

    /**
     * atomic truncate log start 0 to endOffset and update snapshot meta data
     * @param endOffset
     * @param metadata
     */
    void truncatePrefixAndUpdateMeta(long endOffset, RaftProto.SnapshotMetadata metadata);

    /**
     * may need reload data to recover storage state
     */
    void recover();

    /**
     * clear storage
     */
    void clear() throws IOException;
}
