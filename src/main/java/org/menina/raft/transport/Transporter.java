package org.menina.raft.transport;

import org.menina.raft.message.RaftProto;
import org.menina.rail.common.exception.RpcException;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * @author zhenghao
 * @date 2019/2/20
 */
public interface Transporter {

    CompletableFuture<RaftProto.Message> request(RaftProto.Message message);

}
