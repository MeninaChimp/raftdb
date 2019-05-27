package org.menina.raft.election;

/**
 * @author zhenghao
 * @date 2019/1/24
 */
public interface Tick extends Runnable {

    long now();

    void start();

    void addListener(TickListener listener);

    void removeListener(String id);
}
