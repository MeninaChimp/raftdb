package org.menina.raft.common;

import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author zhenghao
 * @date 2019/2/18
 */

@Data
@NoArgsConstructor
public class HardState {

    private Long term;

    private Long committed;

    private Integer voteFor;
}
