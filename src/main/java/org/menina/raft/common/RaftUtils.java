package org.menina.raft.common;

import com.esotericsoftware.yamlbeans.YamlReader;
import com.google.common.base.Preconditions;
import sun.misc.Unsafe;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Field;
import java.security.AccessController;
import java.security.PrivilegedExceptionAction;
import java.util.zip.CRC32;

/**
 * @author zhenghao
 * @date 2019/1/23
 */
public class RaftUtils {

    private static final Unsafe UNSAFE;

    static
    {
        try
        {
            final PrivilegedExceptionAction<Unsafe> action = () -> {
                Field theUnsafe = Unsafe.class.getDeclaredField("theUnsafe");
                theUnsafe.setAccessible(true);
                return (Unsafe) theUnsafe.get(null);
            };

            UNSAFE = AccessController.doPrivileged(action);
        }
        catch (Exception e)
        {
            throw new RuntimeException("Unable to load unsafe", e);
        }
    }

    public static Unsafe unsafe()
    {
        return UNSAFE;
    }

    public static NodeInfo parseAddress(String address) {
        String[] info = address.split(":");
        String host = info[0];
        int port = Integer.parseInt(info[1]);
        int id = Integer.parseInt(info[2]);
        return NodeInfo.builder()
                .host(host)
                .port(port)
                .id(id)
                .build();
    }

    public static File indexFile(String baseDir, long offset) {
        Preconditions.checkNotNull(baseDir);
        Preconditions.checkArgument(offset >= 0);
        return new File(baseDir + File.separator + buildFileName(offset, Constants.INDEX_SUFFIX));
    }

    public static File logFile(String baseDir, long offset) {
        Preconditions.checkNotNull(baseDir);
        Preconditions.checkArgument(offset >= 0);
        return new File(baseDir + File.separator + buildFileName(offset, Constants.LOG_SUFFIX));
    }

    public static File snapFile(String baseDir, long term, long offset) {
        Preconditions.checkNotNull(baseDir);
        Preconditions.checkArgument(offset >= 0);
        return new File(baseDir + File.separator + buildFileNameWithTerm(term, offset, Constants.SNAP_SUFFIX));
    }

    public static File snapTempFile(String baseDir, long term, long offset) {
        Preconditions.checkNotNull(baseDir);
        Preconditions.checkArgument(offset >= 0);
        return new File(baseDir + File.separator + buildFileNameWithTerm(term, offset, Constants.SNAP_TEMP_SUFFIX));
    }

    public static boolean isIndexFile(File file) {
        Preconditions.checkNotNull(file);
        return file.getName().endsWith(Constants.INDEX_SUFFIX);
    }

    public static boolean isLogFile(File file) {
        Preconditions.checkNotNull(file);
        return file.getName().endsWith(Constants.LOG_SUFFIX);
    }

    public static boolean isSnapFile(File file) {
        Preconditions.checkNotNull(file);
        return file.getName().endsWith(Constants.SNAP_SUFFIX);
    }

    public static boolean isSnapTempFile(File file) {
        Preconditions.checkNotNull(file);
        return file.getName().endsWith(Constants.SNAP_TEMP_SUFFIX);
    }

    public static long extractBaseOffset(File file) {
        Preconditions.checkNotNull(file);
        if (!isSnapFile(file) && !isSnapTempFile(file)) {
            return Long.parseLong(file.getName().substring(0, file.getName().indexOf(".")));
        } else {
            return Long.parseLong(file.getName().substring(file.getName().indexOf("-") + 1, file.getName().indexOf(".")));
        }
    }

    public static long extractBaseTerm(File file) {
        Preconditions.checkNotNull(file);
        Preconditions.checkArgument(isSnapFile(file));
        Preconditions.checkArgument(isSnapFile(file) || isSnapTempFile(file));
        return Long.parseLong(file.getName().substring(0, file.getName().indexOf("-")));
    }

    private static String buildFileName(long offset, String suffix) {
        Preconditions.checkArgument(offset >= 0);
        Preconditions.checkNotNull(suffix);
        return String.format("%020d.%s", offset, suffix);
    }

    private static String buildFileNameWithTerm(long term, long offset, String suffix) {
        Preconditions.checkArgument(offset >= 0);
        Preconditions.checkNotNull(suffix);
        return String.format("%010d-%020d.%s", term, offset, suffix);
    }

    public static Long crc(byte[] data) {
        CRC32 crc32 = new CRC32();
        crc32.update(data);
        return crc32.getValue();
    }

    public static boolean checkCrc(byte[] data, long crc) {
        return crc(data).equals(crc);
    }

    public static String getSystemProperty(String key, String defaultValue) {
        Preconditions.checkNotNull(key);
        String result = System.getenv(key);
        if (result == null) {
            result = System.getProperty(key);
        }

        return result == null ? defaultValue : result;
    }

    public static RaftConfig extractConfigFromYml() throws IOException {
        YamlReader reader = null;
        try {
            reader = new YamlReader(new FileReader(RaftUtils.getSystemProperty(Constants.DEFAULT_RAFT_CONFIG_PATH, Constants.DEFAULT_CONFIG_FILE_NAME)));
            return reader.read(RaftConfig.class);
        } finally {
            if (reader != null) {
                reader.close();
            }
        }
    }
}
