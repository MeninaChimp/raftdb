package org.menina.raft.exception;

/**
 * @author zhenghao
 * @date 2019/4/11
 */
public class UnavailableException extends RaftException {

    public UnavailableException() {
    }

    public UnavailableException(String message) {
        super(message);
    }
}
