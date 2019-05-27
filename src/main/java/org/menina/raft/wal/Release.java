package org.menina.raft.wal;

import java.io.IOException;

/**
 * @author zhenghao
 * @date 2019/3/13
 */
public interface Release {

    void close() throws IOException;

    void delete() throws IOException;

}
