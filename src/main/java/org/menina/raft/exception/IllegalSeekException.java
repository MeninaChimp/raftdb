package org.menina.raft.exception;

import java.io.IOException;

/**
 * @author zhenghao
 * @date 2019/4/11
 */
public class IllegalSeekException extends IOException {

    public IllegalSeekException() {
    }

    public IllegalSeekException(String message) {
        super(message);
    }

    public IllegalSeekException(String message, Throwable cause) {
        super(message, cause);
    }
}
