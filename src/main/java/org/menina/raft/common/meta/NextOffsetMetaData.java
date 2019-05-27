package org.menina.raft.common.meta;

import lombok.ToString;

import javax.annotation.concurrent.ThreadSafe;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author zhenghao
 * @date 2019/4/1
 */
@ToString
@ThreadSafe
public class NextOffsetMetaData {

    private AtomicLong offset;

    public NextOffsetMetaData(long offset) {
        this.offset = new AtomicLong(offset);
    }

    public long incrementOffset() {
        return offset.incrementAndGet();
    }

    public void setOffset(long offset) {
        this.offset.set(offset);
    }

    public long getOffset() {
        return offset.get();
    }
}
