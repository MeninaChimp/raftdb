package org.menina.raft.common;

import org.menina.raft.api.State;
import org.menina.raft.transport.Transporter;
import lombok.Builder;
import lombok.Data;
import lombok.ToString;

/**
 * @author zhenghao
 * @date 2019/1/23
 */
@Data
@Builder
@ToString(exclude = "transporter")
public class NodeInfo {

    private int id;

    private String host;

    private int port;

    private long nextIndex;

    private boolean transportSnapshot;

    private volatile boolean snapshot;

    private volatile boolean applying;

    @Builder.Default
    private State.ReplayState replayState = State.ReplayState.UNREPLAY;

    @Builder.Default
    private long matchIndex = Constants.DEFAULT_INIT_OFFSET;

    @Builder.Default
    private volatile long committed = Constants.DEFAULT_INIT_OFFSET;

    @Builder.Default
    private boolean paused = true;

    @Builder.Default
    private boolean disconnected = true;

    private Transporter transporter;
}
