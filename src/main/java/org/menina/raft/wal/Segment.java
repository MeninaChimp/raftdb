package org.menina.raft.wal;

import org.menina.raft.message.RaftProto;

import java.io.IOException;
import java.util.List;

/**
 * @author zhenghao
 * @date 2019/2/26
 *
 * segment represents a collection of data in a range, including data files and index files
 */
public interface Segment extends Release {

    /**
     * append entry to segment
     *
     * @param entry
     * @throws IOException
     */
    void append(RaftProto.Entry entry) throws IOException;

    /**
     * batch append entries to segment
     * @param entries
     * @throws IOException
     */
    void append(List<RaftProto.Entry> entries) throws IOException;

    /**
     * fetch batch data from given offset
     * @param offset
     * @param limit
     * @return
     * @throws IOException
     */
    List<RaftProto.Entry> fetch(long offset, int limit) throws IOException;

    /**
     * check if need to roll a new segment
     * @param appendSize
     * @return
     */
    boolean shouldRoll(int appendSize);

    /**
     * recover segment
     * @throws IOException
     */
    void recover() throws IOException;

    /**
     * check if segment is broken
     */
    void safetyCheck();

    /**
     * get last index for a segment
     * @return
     */
    long lastIndex();

    /**
     * get last entry for a segment
     * @return
     * @throws IOException
     */
    RaftProto.Entry lastEntry() throws IOException;

    /**
     * recover state for a segment
     * @throws IOException
     */
    void loadState() throws IOException;

    /**
     * truncate log start from given offset, the given offset will be included
     * @param offset
     * @throws IOException
     */
    void truncate(long offset) throws IOException;

    /**
     * flush segment to disk
     * @param metaData
     * @throws IOException
     */
    void flush(boolean metaData) throws IOException;

}
