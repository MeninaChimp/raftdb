package org.menina.raft.common;

import org.menina.raft.message.RaftProto;
import org.menina.raft.snapshot.SnapshotFuture;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * @author zhenghao
 * @date 2019/2/18
 */
@Data
@NoArgsConstructor
public class Ready {

    int entriesSize;

    int committedEntriesSize;

    int messageSize;

    List<RaftProto.Entry> entries;

    List<RaftProto.Entry> committedEntries;

    List<RaftProto.Message> messages;

    SnapshotFuture snapshot;

    boolean mustSync;

    HardState state;

    public void setEntries(List<RaftProto.Entry> entries) {
        this.entries = entries;
        this.entriesSize = entries.size();
    }

    public void setCommittedEntries(List<RaftProto.Entry> committedEntries) {
        this.committedEntries = committedEntries;
        this.committedEntriesSize = committedEntries == null ? 0 : committedEntries.size();
    }

    public void setMessages(List<RaftProto.Message> messages) {
        this.messages = messages;
        this.messageSize = messages.size();
    }

    public boolean validate() {
        return entriesSize > 0 || committedEntriesSize > 0 || messageSize > 0 || snapshot != null;
    }

    public boolean needApply(){
        return committedEntriesSize > 0 || snapshot != null;
    }
}
