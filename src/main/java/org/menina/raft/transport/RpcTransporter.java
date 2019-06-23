package org.menina.raft.transport;

import com.google.common.base.Preconditions;
import org.menina.raft.common.Event;
import org.menina.raft.core.RequestChannel;
import org.menina.raft.message.RaftProto;
import  org.menina.rail.common.annotation.Exporter;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CompletableFuture;

/**
 * @author zhenghao
 * @date 2019/2/20
 */
@Slf4j
@Exporter
@AllArgsConstructor
public class RpcTransporter implements Transporter {

    private RequestChannel requestChannel;

    @Override
    public CompletableFuture<RaftProto.Message> request(RaftProto.Message message) {
        Preconditions.checkNotNull(message);
        switch (message.getType()) {
            case PROPOSE:
                requestChannel.publish(Event.builder()
                        .type(RaftProto.EventType.PROPOSAL)
                        .value(message)
                        .build());
                break;
            default:
                requestChannel.publish(Event.builder()
                        .type(RaftProto.EventType.MESSAGE)
                        .value(message)
                        .build());
        }

        return null;
    }
}
