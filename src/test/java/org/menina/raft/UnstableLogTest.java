package org.menina.raft;

import com.google.protobuf.UnsafeByteOperations;
import org.menina.raft.log.UnstableLog;
import org.menina.raft.message.RaftProto;

import java.util.ArrayList;
import java.util.List;

/**
 * @author zhenghao
 * @date 2019/4/1
 */
public class UnstableLogTest {

    public static void main(String[] args) {
        UnstableLog unstableLog = new UnstableLog();
        List<RaftProto.Entry> entries = new ArrayList<>();
        RaftProto.Entry entry = RaftProto.Entry.newBuilder().setIndex(1).setTerm(1).setData(UnsafeByteOperations.unsafeWrap("1".getBytes())).build();
        entries.add(entry);
        unstableLog.append(entries);
        List<RaftProto.Entry> fetch = unstableLog.entries(0, 1);
        System.out.println(fetch.size());
    }
}
