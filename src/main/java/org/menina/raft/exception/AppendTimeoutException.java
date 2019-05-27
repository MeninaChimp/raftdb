package org.menina.raft.exception;

import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * @author zhenghao
 * @date 2019/4/11
 */
@Data
@EqualsAndHashCode(callSuper = true)
public class AppendTimeoutException extends RaftException {

    private byte[] data;

    public AppendTimeoutException(byte[] data) {
        this.data = data;
    }

    public AppendTimeoutException(String message, byte[] data) {
        super(message);
        this.data = data;
    }
}
