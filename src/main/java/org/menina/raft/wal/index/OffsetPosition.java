package org.menina.raft.wal.index;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @author zhenghao
 * @date 2019/3/6
 */
@Data
@AllArgsConstructor
public class OffsetPosition {

    private long offset;
    private int position;
}
