package org.menina.raft.statemachine;

import org.menina.raft.message.RaftProto;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * @author zhenghao
 * @date 2019/2/25
 */
public interface StateMachine {

    void apply(List<RaftProto.Entry> entries);

    ByteBuffer buildsSnapshot();

    void applySnapshot(ByteBuffer data);

}
