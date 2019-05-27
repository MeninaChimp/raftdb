package org.menina.raft.snapshot;

import com.google.common.base.Preconditions;
import com.google.protobuf.UnsafeByteOperations;
import org.menina.raft.api.Node;
import org.menina.raft.common.RaftUtils;
import org.menina.raft.message.RaftProto;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * @author zhenghao
 * @date 2019/4/11
 */
@Slf4j
@NotThreadSafe
public class DefaultSnapshotter implements Snapshotter {

    private Node raftNode;
    private boolean recovered;

    public DefaultSnapshotter(Node raftNode) {
        Preconditions.checkNotNull(raftNode);
        this.raftNode = raftNode;
    }

    @Override
    public void save(RaftProto.Snapshot snapshot) throws IOException {
        Preconditions.checkNotNull(snapshot.getMeta());
        Preconditions.checkNotNull(snapshot.getMeta().getTerm() > 0);
        Preconditions.checkNotNull(snapshot.getMeta().getIndex() > 0);
        long begin = raftNode.clock().now();
        File snapTemp = RaftUtils.snapTempFile(raftNode.config().getSnap(), snapshot.getMeta().getTerm(), snapshot.getMeta().getIndex());
        if (!snapTemp.createNewFile()) {
            throw new IllegalStateException("failed to create " + snapTemp);
        }

        ByteBuffer buffer = snapshot.getData().asReadOnlyByteBuffer();
        log.info("start save snapshot {}, size {} KB", snapTemp.getAbsolutePath(), buffer.limit() >> 10);
        try (RandomAccessFile appender = new RandomAccessFile(snapTemp, "rw")) {
            appender.getChannel().write(buffer);
        }

        File snap = RaftUtils.snapFile(raftNode.config().getSnap(), snapshot.getMeta().getTerm(), snapshot.getMeta().getIndex());
        Files.move(snapTemp.toPath(), snap.toPath(), StandardCopyOption.REPLACE_EXISTING);
        log.info("start to truncate storage end with index {}", snapshot.getMeta().getIndex());
        raftNode.storage().truncatePrefixAndUpdateMeta(snapshot.getMeta().getIndex(), snapshot.getMeta());
        long mills = (raftNode.clock().now() - begin) * raftNode.config().getClockAccuracyMills();
        log.info("save snapshot {} success, time cost {} ms", snap.getAbsolutePath(), mills);
    }

    @Override
    public RaftProto.Snapshot loadNewest() throws IOException {
        Preconditions.checkArgument(recovered);
        File snapDir = new File(raftNode.config().getSnap());
        File[] snaps = snapDir.listFiles();
        NavigableMap<Long, File> snapMap = new TreeMap<Long, File>();
        if (snaps != null) {
            for (File snap : snaps) {
                snapMap.put(RaftUtils.extractBaseOffset(snap), snap);
            }
        }

        if (snapMap.isEmpty()) {
            return null;
        }

        File snap = snapMap.lastEntry().getValue();
        try (RandomAccessFile reader = new RandomAccessFile(snap, "r")) {
            RaftProto.SnapshotMetadata metadata = RaftProto.SnapshotMetadata
                    .newBuilder()
                    .setIndex(RaftUtils.extractBaseOffset(snap))
                    .setTerm(RaftUtils.extractBaseTerm(snap))
                    .build();

            int length = (int) reader.length();
            byte[] data = new byte[length];
            reader.read(data, 0, length);
            log.info("load snapshot {} success", snap.getName());
            return RaftProto.Snapshot
                    .newBuilder()
                    .setMeta(metadata)
                    .setData(UnsafeByteOperations.unsafeWrap(data))
                    .build();
        }
    }

    @Override
    public void recover() {
        File snapDir = new File(raftNode.config().getSnap());
        if (!snapDir.exists()) {
            if (snapDir.mkdirs()) {
                log.info("create snapshot directory {} success", snapDir.getAbsolutePath());
            } else {
                throw new IllegalStateException("failed to create directory " + snapDir.getAbsolutePath());
            }
        } else {
            File[] snaps = snapDir.listFiles();
            if (snaps != null) {
                if (snaps.length > 0) {
                    log.info("start recover snapshots {}", (Object) snaps);
                }

                for (File snap : snaps) {
                    if (RaftUtils.isSnapTempFile(snap)) {
                        log.warn("detect snap temp file {} in dir {}, most likely experienced an accidental downtime, remove temp file", snap.getName(), snapDir.getName());
                        if (!snap.delete()) {
                            log.error("failed to remove snap temp file {}, check permission or disk status please", snap.getName());
                        }
                    }
                }
            } else {
                throw new IllegalStateException("invalid file path for '\u0000',  please rename file for recover success");
            }
        }

        recovered = true;
    }

    @Override
    public NavigableMap<Long, RaftProto.SnapshotMetadata> snapshots() {
        Preconditions.checkArgument(recovered);
        NavigableMap<Long, RaftProto.SnapshotMetadata> snapshots = new TreeMap<>();
        File snapDir = new File(raftNode.config().getSnap());
        File[] snaps = snapDir.listFiles();
        if (snaps != null) {
            for (File snap : snaps) {
                if (RaftUtils.isSnapTempFile(snap)) {
                    log.warn("detect snap temp file {} in dir {}, most likely experienced an accidental downtime, remove temp file", snap.getName(), snapDir.getName());
                    if (!snap.delete()) {
                        log.error("failed to remove snap temp file {}, check permission or disk status please", snap.getName());
                    }
                } else {
                    long offset = RaftUtils.extractBaseOffset(snap);
                    snapshots.put(offset, RaftProto.SnapshotMetadata.newBuilder()
                            .setIndex(offset)
                            .setTerm(RaftUtils.extractBaseTerm(snap))
                            .build());
                }
            }
        }

        return snapshots;
    }
}
