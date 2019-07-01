package org.menina.raft.common;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import sun.misc.Unsafe;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.List;

/**
 * @author zhenghao
 * @date 2019/6/4
 */
@NotThreadSafe
public class RingBuffer<T> {

    private static final int BUFFER_PAD;
    private static final int REF_ELEMENT_SHIFT;
    private static final int REF_ARRAY_BASE;
    private static final int CACHE_LINE = 64;
    private static final Unsafe UNSAFE = RaftUtils.unsafe();

    static {
        int scale = UNSAFE.arrayIndexScale(Object[].class);
        if (4 == scale) {
            REF_ELEMENT_SHIFT = 2;
        } else if (8 == scale) {
            REF_ELEMENT_SHIFT = 3;
        } else {
            throw new IllegalStateException("Unknown pointer size");
        }

        BUFFER_PAD = CACHE_LINE / scale;
        REF_ARRAY_BASE = UNSAFE.arrayBaseOffset(Object[].class) + (BUFFER_PAD << REF_ELEMENT_SHIFT);
    }

    private int indexMask;
    private Object[] buffer;
    private long highWatermark = Constants.DEFAULT_INIT_OFFSET;

    public RingBuffer(int bufferSize) {
        Preconditions.checkArgument(bufferSize > 0);
        Preconditions.checkArgument(Integer.bitCount(bufferSize) == 1, "bufferSize must be a power of 2");
        buffer = new Object[bufferSize + BUFFER_PAD << 1];
        indexMask = bufferSize - 1;
    }

    public void append(long index, T entry) {
        Preconditions.checkArgument(index > highWatermark);
        this.highWatermark = index;
        UNSAFE.putObject(buffer, REF_ARRAY_BASE + (highWatermark & indexMask) << REF_ELEMENT_SHIFT, entry);
    }

    @SuppressWarnings("unchecked")
    public T entry(long index) {
        Preconditions.checkArgument(index <= highWatermark);
        return (T) UNSAFE.getObject(buffer, REF_ARRAY_BASE + (index & indexMask) << REF_ELEMENT_SHIFT);
    }

    public List<T> entries(long begin, long end) {
        Preconditions.checkArgument(end <= highWatermark);
        Preconditions.checkArgument(begin >= lowWatermark());
        List<T> entries = Lists.newArrayList();
        for (long i = begin; i <= end; i++) {
            entries.add(entry(i));
        }

        return entries;
    }

    public void truncate(long index){
        Preconditions.checkArgument(index <= highWatermark);
        Preconditions.checkArgument(index >= lowWatermark());
        highWatermark = index;
    }

    public void clear(){
        highWatermark = Constants.DEFAULT_INIT_OFFSET;
    }

    public long highWatermark() {
        return highWatermark;
    }

    public long lowWatermark() {
        if (highWatermark == Constants.DEFAULT_INIT_OFFSET) {
            return Constants.DEFAULT_INIT_OFFSET;
        }

        long lowWatermark = highWatermark - indexMask;
        return lowWatermark > 0 ? lowWatermark : 0;
    }
}

