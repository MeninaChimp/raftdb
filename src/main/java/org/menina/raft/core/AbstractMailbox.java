package org.menina.raft.core;

import com.google.common.collect.Lists;
import org.menina.raft.api.Mailbox;
import org.menina.raft.message.RaftProto;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.concurrent.ThreadSafe;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author zhenghao
 * @date 2019/2/20
 */
@Slf4j
@ThreadSafe
public abstract class AbstractMailbox implements Mailbox {

    private List<RaftProto.Message> mailbox = Lists.newArrayList();
    private ReadWriteLock lock = new ReentrantReadWriteLock();
    private Lock readLock = lock.readLock();
    private Lock writeLock = lock.writeLock();

    @Override
    public void send(RaftProto.Message message) {
        readLock.lock();
        try {
            mailbox.add(message);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public synchronized List<RaftProto.Message> drain() {
        writeLock.lock();
        try {
            List<RaftProto.Message> drain = mailbox;
            mailbox = Lists.newArrayList();
            return drain;
        } finally {
            writeLock.unlock();
        }
    }
}
