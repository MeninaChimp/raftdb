package org.menina.raft.api;

import org.menina.raft.common.Ready;
import org.menina.raft.election.CampaignType;
import org.menina.raft.message.RaftProto;

/**
 * @author zhenghao
 * @date 2019/2/11
 */
public interface RaftApis {

    /**
     * all event handle
     *
     * @param message
     * @return
     */
    void handleEvent(RaftProto.Message message);

    /**
     * campaign
     *
     * @param type
     */
    void campaign(CampaignType type);

    /**
     * group commit
     * @return
     */
    Ready newReady();

    /**
     * call state machine to build snapshot
     */
    void triggerToSnapshot();
}
