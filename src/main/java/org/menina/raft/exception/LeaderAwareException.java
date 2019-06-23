package org.menina.raft.exception;

import com.google.common.base.Preconditions;
import lombok.Getter;

/**
 * @author zhenghao
 * @date 2019/2/12
 */
@Getter
public class LeaderAwareException extends RaftException {

    private boolean aware;
    private int id;
    private String address;
    private int port;

    public LeaderAwareException(String message) {
        super(message);
    }

    public LeaderAwareException(int id, String address, int port) {
        Preconditions.checkArgument(id >= 0);
        Preconditions.checkNotNull(address);
        Preconditions.checkArgument(port > 0);
        this.aware = true;
        this.id = id;
        this.address = address;
        this.port = port;
    }
}