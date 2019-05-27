package org.menina.raft.core.loop;

/**
 * @author zhenghao
 * @date 2019/2/25
 */
public interface EventLoop extends Runnable {

    void close();

}
