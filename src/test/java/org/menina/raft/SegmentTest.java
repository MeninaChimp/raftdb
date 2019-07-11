package org.menina.raft;

import com.google.protobuf.UnsafeByteOperations;
import org.menina.raft.common.Constants;
import org.menina.raft.common.RaftUtils;
import org.menina.raft.message.RaftProto;
import org.menina.raft.wal.LogSegment;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * @author zhenghao
 * @date 2019/3/7
 */
public class SegmentTest {

    public static void main(String[] args) throws IOException {

        long baseOffset = 0;
        LogSegment segment = new LogSegment(Constants.DEFAULT_WAL_DIR, baseOffset, 60 * 60 * 1000L, 512 * 1024 * 1024L, 8192);
        List<RaftProto.Entry> entries = new ArrayList<>();
        ByteBuffer buffer = ByteBuffer.allocate(512 * 8);
        for (Long i = 1L; i <= 64L * 8; i++) {
            buffer.putLong(i);
        }

        for (Integer i = 0; i < 50000; i++) {
            buffer.flip();
            RaftProto.Entry entry = RaftProto.Entry.newBuilder()
                    .setIndex(i)
                    .setData(UnsafeByteOperations.unsafeWrap(buffer))
                    .setCrc(RaftUtils.crc(buffer.array()))
                    .build();
            entries.add(entry);
        }

        long current = System.currentTimeMillis();
        segment.append(entries);
        System.out.println("200MB 4K write cost = " + (System.currentTimeMillis() - current) + "ms");
        List<RaftProto.Entry> fetchData = new ArrayList<>();
        current = System.currentTimeMillis();
        fetchData = segment.fetch(0, 50000);
        System.out.println("200MB 4K read cost = " + (System.currentTimeMillis() - current) + "ms");
        System.out.println(fetchData.size());
        System.out.println(fetchData.get(49999).getData());
    }
}
