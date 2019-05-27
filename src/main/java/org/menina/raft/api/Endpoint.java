package org.menina.raft.api;

import java.util.concurrent.CompletableFuture;

/**
 * @author zhenghao
 * @date 2019/1/23
 */
public interface Endpoint {

    void start();

    void close();

    CompletableFuture closeFuture();
}
