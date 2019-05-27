package org.menina.raft.wal.index;

import com.google.common.base.Preconditions;
import org.menina.raft.common.Constants;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;

/**
 * @author zhenghao
 * @date 2019/3/7
 */
@NotThreadSafe
public class OffsetIndex extends AbstractIndex {

    private boolean sparse;
    private int lastAppendPosition;

    public OffsetIndex(String baseDir, long baseOffset, long maxFileSize, boolean readOnly, boolean sparse) throws IOException {
        super(baseDir, baseOffset, maxFileSize, readOnly);
        this.sparse = sparse;
    }

    @Override
    public void append(long offset, int position) {
        if (!allowAppendIndex(position)) {
            return;
        }

        Preconditions.checkArgument(offset >= this.baseOffset);
        Preconditions.checkArgument(position >= 0);
        this.mappedByteBuffer.putInt(relativeOffset(offset));
        this.mappedByteBuffer.putInt(position);
        this.entries++;
        this.lastAppendPosition = position;
        this.dirty = true;
    }

    @Override
    public int entrySize() {
        return Integer.BYTES << 1;
    }

    @Override
    public void safetyCheck() {
        if (!file.exists()) {
            throw new IllegalStateException("lost index file for offset " + baseOffset + ", redo index");
        }

        if (entries > 0 && parseEntry(entries - 1).getOffset() <= baseOffset) {
            throw new IllegalStateException("unfilled index file for offset " + baseOffset + ", redo index");
        }
    }

    private boolean allowAppendIndex(int position) {
        return !sparse || position - lastAppendPosition > Constants.INDEX_INTERVAL_BYTES;
    }
}
