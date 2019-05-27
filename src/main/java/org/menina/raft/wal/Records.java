package org.menina.raft.wal;

import org.menina.raft.message.RaftProto;

import java.io.IOException;
import java.util.List;

/**
 * @author zhenghao
 * @date 2019/3/13
 */
public interface Records extends Release {

    int OFFSET_OFFSET = 0;
    int OFFSET_LENGTH = Long.BYTES;
    int RESERVED_OFFSET = OFFSET_OFFSET + OFFSET_LENGTH;
    int RESERVED_LENGTH = Long.BYTES;
    int SIZE_OFFSET = RESERVED_OFFSET + RESERVED_LENGTH;
    int SIZE_LENGTH = Integer.BYTES;
    int HEADER_OFFSET = SIZE_OFFSET + SIZE_LENGTH;

    /**
     * append entry to data file
     * @param entry
     * @return
     * @throws IOException
     */
    int append(RaftProto.Entry entry) throws IOException;

    /**
     * seek to position, read from given offset
     *
     * @param position index file given position
     * @param offset target offset
     * @param maxSize batch size
     * @return
     * @throws IOException
     */
    List<RaftProto.Entry> read(int position, long offset, int maxSize) throws IOException;

    /**
     * seek to position with bound check
     * @param position
     * @param offset
     * @return
     * @throws IOException
     */
    int seekTo(int position, long offset) throws IOException;

    /**
     * check current data file can append more data
     * @param appendSize
     * @return
     */
    boolean canAppend(int appendSize);

    /**
     * flush to disk
     * @param metaData
     * @throws IOException
     */
    void flush(boolean metaData) throws IOException;

    /**
     * get last modified for data file
     * @return
     */
    long largestTimestamp();

    /**
     * truncate data file to given position
     * @param position
     * @return
     * @throws IOException
     */
    int truncate(int position) throws IOException;

    /**
     * get last offset for data file
     * @return
     */
    long lastOffset();

    /**
     * update last offset for data file
     * @param lastOffset
     */
    void updateLastOffset(long lastOffset);

}
