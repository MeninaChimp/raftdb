package org.menina.raft.common;

import lombok.extern.slf4j.Slf4j;

/**
 * @author zhenghao
 * @date 2019/4/11
 */
@Slf4j
public class RaftThread extends Thread {

    public static RaftThread daemon(Runnable command, String name) {
        return new RaftThread(command, name, true);
    }

    public static RaftThread nonDaemon(Runnable command, String name) {
        return new RaftThread(command, name, false);
    }

    private RaftThread(Runnable command, String name, boolean daemon) {
        super(command, name);
        configuration(daemon);
    }

    private void configuration(boolean daemon) {
        setDaemon(daemon);
        setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
            @Override
            public void uncaughtException(Thread t, Throwable e) {
                log.error("Uncaught Exception for thread {}, error message: {}",
                        t.getName(),
                        e.getMessage(),
                        e);
            }
        });
    }
}
