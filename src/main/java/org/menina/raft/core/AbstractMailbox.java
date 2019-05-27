package org.menina.raft.core;

import com.google.common.collect.Lists;
import org.menina.raft.api.Mailbox;
import org.menina.raft.message.RaftProto;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.List;

/**
 * @author zhenghao
 * @date 2019/2/20
 */
@NotThreadSafe
public abstract class AbstractMailbox implements Mailbox {

    private List<RaftProto.Message> mailbox = Lists.newArrayList();

    @Override
    public void send(RaftProto.Message message) {
        mailbox.add(message);
    }

    @Override
    public List<RaftProto.Message> drain() {
        List<RaftProto.Message> drain = mailbox;
        mailbox = Lists.newArrayList();
        return drain;
    }
}
