package org.menina.raft.api;

import org.menina.raft.message.RaftProto;

import java.util.List;

/**
 * @author zhenghao
 * @date 2019/2/20
 *
 * interactive information buffer
 */
public interface Mailbox {

    void send(RaftProto.Message message);

    List<RaftProto.Message> drain();

}
