package com.messaging.storage.segment;

import com.sun.jna.Library;
import com.sun.jna.Native;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileDescriptor;
import java.lang.reflect.Field;
import java.nio.channels.FileChannel;

/**
 * Advises the kernel to drop page cache after a segment is sealed and fsynced.
 *
 * Without this, every sealed segment's pages remain warm in the cgroup's memory
 * budget (Docker counts page cache against the container). On a replay-heavy
 * startup the broker can push 600+ MB of clean pages into cache before the
 * kernel self-evicts under pressure, risking the 700 MB container limit.
 *
 * Requires --add-opens java.base/sun.nio.ch=ALL-UNNAMED and
 *          --add-opens java.base/java.io=ALL-UNNAMED in JAVA_OPTS.
 * Gracefully disables itself on non-Linux or if those opens are absent.
 */
final class NativePageCache {
    private static final Logger log = LoggerFactory.getLogger(NativePageCache.class);
    private static final int POSIX_FADV_DONTNEED = 4;

    private interface Libc extends Library {
        int posix_fadvise(int fd, long offset, long len, int advice);
    }

    private static final boolean ENABLED;
    private static Libc libc;
    private static Field channelFdField;
    private static Field fdIntField;

    static {
        boolean ok = false;
        if (System.getProperty("os.name", "").toLowerCase().contains("linux")) {
            try {
                libc = Native.load("c", Libc.class);
                channelFdField = Class.forName("sun.nio.ch.FileChannelImpl").getDeclaredField("fd");
                channelFdField.setAccessible(true);
                fdIntField = FileDescriptor.class.getDeclaredField("fd");
                fdIntField.setAccessible(true);
                ok = true;
                log.info("posix_fadvise DONTNEED enabled — page cache will be dropped after segment seal");
            } catch (Exception e) {
                log.debug("posix_fadvise unavailable ({}); add --add-opens java.base/sun.nio.ch=ALL-UNNAMED "
                        + "--add-opens java.base/java.io=ALL-UNNAMED to JAVA_OPTS to enable", e.getMessage());
            }
        }
        ENABLED = ok;
    }

    static void dropCache(FileChannel channel) {
        if (!ENABLED || channel == null) return;
        try {
            FileDescriptor fd = (FileDescriptor) channelFdField.get(channel);
            int nativeFd = (int) fdIntField.get(fd);
            libc.posix_fadvise(nativeFd, 0, 0, POSIX_FADV_DONTNEED);
        } catch (Exception e) {
            log.debug("posix_fadvise failed: {}", e.getMessage());
        }
    }

    private NativePageCache() {}
}
