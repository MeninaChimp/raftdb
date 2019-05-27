package org.menina.raft.exception;

/**
 * @author zhenghao
 * @date 2019/2/11
 */
public class RaftException extends Exception {

    public RaftException() {
    }

    public RaftException(String message) {
        super(message);
    }

    public RaftException(String message, Throwable cause) {
        super(message, cause);
    }
}
