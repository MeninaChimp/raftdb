package org.menina.raft;

import com.google.protobuf.UnsafeByteOperations;
import org.menina.raft.common.RaftConfig;
import org.menina.raft.common.RaftUtils;
import org.menina.raft.message.RaftProto;
import org.menina.raft.wal.Wal;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * @author zhenghao
 * @date 2019/3/19
 */
public class WalTest {

    private static final String cluster = "127.0.0.1:1011:1 127.0.0.1:1012:2 127.0.0.1:1013:3";
    private static final int id = 1;

    public static void main(String[] args) throws IOException {
        RaftConfig config = RaftConfig
                .builder()
                .cluster(cluster)
                .id(id)
                .build();

        List<RaftProto.Entry> entries = new ArrayList<>();
        ByteBuffer buffer = ByteBuffer.allocate(512 * 8);
        for (Long i = 1L; i <= 64L * 8; i++) {
            buffer.putLong(i);
        }

        for (Integer i = 0; i < 10000; i++) {
            buffer.flip();
            RaftProto.Entry entry = RaftProto.Entry
                    .newBuilder()
                    .setIndex(i)
                    .setData(UnsafeByteOperations.unsafeWrap(buffer))
                    .setCrc(RaftUtils.crc(buffer.array()))
                    .build();

            entries.add(entry);
        }


        Wal wal = new Wal(config);
        long current = System.currentTimeMillis();
        wal.write(entries);
        System.out.println("40MB 4K write cost = " + (System.currentTimeMillis() - current) + "ms");
    }
}
