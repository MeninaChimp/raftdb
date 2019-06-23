package org.menina.raft.election;

import com.google.common.base.Preconditions;
import  org.menina.rail.common.NamedThreadFactory;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * @author zhenghao
 * @date 2019/1/24
 *
 * 1.global logical clock
 * 2.event generator
 */
@Slf4j
public class LogicalClock implements Tick {

    private long now;
    private long accuracy;
    private AtomicBoolean running = new AtomicBoolean(false);
    private ConcurrentMap<String, TickListener> tickListeners = new ConcurrentHashMap<>();
    private LinkedBlockingQueue<TickEvent> eventsQueue = new LinkedBlockingQueue<TickEvent>(1);
    private ScheduledExecutorService ticker = new ScheduledThreadPoolExecutor(1, new NamedThreadFactory("tick-schedule-thread"));
    private ExecutorService executor = new ThreadPoolExecutor(
            1,
            2,
            30L,
            TimeUnit.SECONDS,
            new SynchronousQueue<>(),
            new NamedThreadFactory("tick-event-listener-thread"));


    public LogicalClock(long accuracy) {
        this.accuracy = accuracy;
    }

    @Override
    public long now() {
        return now;
    }

    @Override
    public void start() {
        if (running.compareAndSet(false, true)) {
            log.info("Global clock running");
            ticker.scheduleAtFixedRate(this, accuracy, accuracy, TimeUnit.MILLISECONDS);
            executor.execute(new Runnable() {
                @Override
                public void run() {
                    while (running.get()) {
                        tickListeners.values().iterator().forEachRemaining(new Consumer<TickListener>() {
                            @Override
                            public void accept(TickListener listener) {
                                try {
                                    listener.onTick(eventsQueue.take());
                                } catch (Exception e) {
                                    log.error(e.getMessage(), e);
                                }
                            }
                        });
                    }

                    log.warn("tick-event-listener-thread shut down");
                }
            });
        } else {
            log.warn("Tick has started");
        }
    }

    @Override
    public void addListener(TickListener listener) {
        Preconditions.checkNotNull(listener.id());
        tickListeners.put(listener.id(), listener);
    }

    @Override
    public void removeListener(String id) {
        tickListeners.remove(id);
    }

    @Override
    public void run() {
        now++;
        if (!eventsQueue.offer(new TickEvent(now)) && tickListeners.size() != 0) {
            log.warn("Miss trigger tick event, make sure non-blocking");
        }
    }
}
