package org.menina.raft.election;

/**
 * @author zhenghao
 * @date 2019/1/24
 */
public interface TickListener {

    String id();

    void onTick(TickEvent event);

    void reset();
}
