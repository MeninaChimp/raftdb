package org.menina.raft.mock;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.menina.raft.common.ByteBufferOutputStream;
import org.menina.raft.common.Constants;
import org.menina.raft.message.RaftProto;
import org.menina.raft.statemachine.StateMachine;
import lombok.extern.slf4j.Slf4j;

import java.io.DataOutputStream;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * @author zhenghao
 * @date 2019/3/27
 */
@Slf4j
public class MockStateMachine implements StateMachine {

    private Map<Long, String> map = Maps.newHashMap();
    private long index = Constants.DEFAULT_INIT_OFFSET;
    private boolean isLeader;

    public void setLeader(boolean leader) {
        isLeader = leader;
    }

    @Override
    public void apply(List<RaftProto.Entry> entries) {
        entries.iterator().forEachRemaining(new Consumer<RaftProto.Entry>() {
            @Override
            public void accept(RaftProto.Entry entry) {
//                if (index == Constants.DEFAULT_INIT_OFFSET) {
//                    index = entry.getIndex();
//                }
//                if (index != entry.getIndex()) {
//                    log.error("no-sequential entry apply to state machine, index {}, correct index {}", entry.getIndex(), index);
//                }

                if (index % 500 == 0 && isLeader) {
                    log.info("index: {}, term: {}, data {}, attach {}", entry.getIndex(), entry.getTerm(), new String(entry.getData().toByteArray()), entry.getAttachmentsMap());
                }

                map.put(entry.getIndex(), new String(entry.getData().toByteArray()));
                index++;
            }
        });
    }

    @Override
    public ByteBuffer buildsSnapshot() {
        ByteBufferOutputStream bufferStream = new ByteBufferOutputStream(10 * 1024 * 1024);
        try (DataOutputStream stream = new DataOutputStream(bufferStream)) {
            for (Map.Entry<Long, String> entry : map.entrySet()) {
                stream.writeLong(entry.getKey());
                byte[] data = entry.getValue().getBytes();
                stream.writeInt(data.length);
                stream.write(data);
            }

            ByteBuffer buffer = bufferStream.buffer();
            buffer.flip();
            return buffer;
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    @Override
    public void applySnapshot(ByteBuffer data) {
        Preconditions.checkNotNull(map);
        while (data.hasRemaining()) {
            long key = data.getLong();
            int length = data.getInt();
            byte[] valueArr = new byte[length];
            data.get(valueArr);
            map.put(key, new String(valueArr));
        }
    }
}
