package org.menina.raft.core;

import com.google.common.base.Preconditions;
import org.menina.raft.common.Apply;
import org.menina.raft.common.Constants;
import org.menina.raft.common.Event;
import org.menina.raft.common.Ready;
import org.menina.raft.message.RaftProto;
import lombok.Data;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.concurrent.ThreadSafe;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author zhenghao
 * @date 2019/2/20
 *
 * 1.pending message
 * 2.event loop interaction
 */
@Slf4j
@ThreadSafe
@Data
@ToString
public class RequestChannel {

    private volatile boolean ready = false;

    private volatile boolean advance = false;

    private volatile boolean canCommit = false;

    private volatile boolean canApply = false;

    private AtomicLong received = new AtomicLong(0);

    private AtomicLong proposed = new AtomicLong(0);

    private AtomicLong applied = new AtomicLong(0);

    private volatile long stableTo = Constants.DEFAULT_INIT_OFFSET;

    private Lock readyLock = new ReentrantLock();

    private Condition readySemaphore = readyLock.newCondition();

    private Lock applyLock = new ReentrantLock();

    private Condition applySemaphore = applyLock.newCondition();

    private BlockingQueue<RaftProto.Message> recvChannel = new ArrayBlockingQueue<>(Constants.RECEIVE_BUF_SIZE);

    private BlockingQueue<RaftProto.Message> propChannel = new ArrayBlockingQueue<>(Constants.MAX_PENDING_PROPOSAL);

    private BlockingQueue<Ready> readyChannel = new ArrayBlockingQueue<>(Constants.MAX_READY_SIZE);

    private BlockingQueue<Object> advanceChannel = new ArrayBlockingQueue<>(Constants.MAX_READY_SIZE);

    private BlockingQueue<Apply> applyChannel = new ArrayBlockingQueue<>(Constants.MAX_APPLY_SIZE);

    public boolean canFetchMessage() {
        return received.get() > 0;
    }

    public boolean canFetchPropose() {
        return proposed.get() > 0;
    }

    public boolean canFetchApply() {
        return applied.get() > 0;
    }

    public void publish(Event event) {
        Preconditions.checkNotNull(event);
        switch (event.getType()) {
            case PROPOSAL:
                if (!propChannel.offer((RaftProto.Message) event.getValue())) {
                    try {
                        propChannel.put((RaftProto.Message) event.getValue());
                    } catch (InterruptedException e) {
                        log.error(e.getMessage());
                    }
                }

                proposed.incrementAndGet();
                break;
            case MESSAGE:
                if (!recvChannel.offer((RaftProto.Message) event.getValue())) {
                    try {
                        recvChannel.put((RaftProto.Message) event.getValue());
                    } catch (InterruptedException e) {
                        log.error(e.getMessage());
                    }
                }

                received.incrementAndGet();
                break;
            case READY:
                readyChannel.offer((Ready) event.getValue());
                break;
            case ADVANCE:
                advanceChannel.offer(event.getValue());
                break;
            case APPLY:
                if (!applyChannel.offer((Apply) event.getValue())) {
                    try {
                        log.warn("snapshot take too much time, will block group commit loop for apply, will cause process slow down");
                        applyChannel.put((Apply) event.getValue());
                    } catch (InterruptedException e) {
                        log.error(e.getMessage());
                    }
                }

                applied.incrementAndGet();
                break;
            default:
                throw new UnsupportedOperationException();
        }
    }

    public Object poll(RaftProto.EventType type, long timeOut, TimeUnit timeUnit) {
        try {
            switch (type) {
                case MESSAGE:
                    Object message = recvChannel.poll(timeOut, timeUnit);
                    if (message != null) {
                        received.decrementAndGet();
                    }

                    return message;
                case PROPOSAL:
                    Object propose = propChannel.poll(timeOut, timeUnit);
                    if (propose != null) {
                        proposed.decrementAndGet();
                    }

                    return propose;
                case READY:
                    return readyChannel.poll(timeOut, timeUnit);
                case ADVANCE:
                    return advanceChannel.poll(timeOut, timeUnit);
                case APPLY:
                    Object apply = applyChannel.poll(timeOut, timeUnit);
                    if (apply != null) {
                        applied.decrementAndGet();
                    }

                    return apply;
                default:
                    throw new UnsupportedOperationException();
            }
        } catch (InterruptedException e) {
            log.error(e.getMessage(), e);
            return null;
        }
    }
}
