package org.menina.raft.common;

import org.menina.raft.message.RaftProto;
import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

/**
 * @author zhenghao
 * @date 2019/2/20
 */
@Getter
@Builder
@ToString
public class Event<T> {

    private RaftProto.EventType type;

    private T value;
}
