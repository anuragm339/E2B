package com.messaging.broker.monitoring;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micronaut.context.annotation.Value;
import io.micronaut.scheduling.annotation.Scheduled;
import jakarta.annotation.PostConstruct;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Container-aware resource metrics intended to align more closely with Docker stats
 * than JVM-only gauges like heap usage.
 */
@Singleton
public class ContainerResourceMonitor {
    private static final Logger log = LoggerFactory.getLogger(ContainerResourceMonitor.class);

    private final MeterRegistry registry;
    private final Path cgroupRoot;
    private final Path dataDir;

    private final AtomicLong memoryCurrentBytes = new AtomicLong(-1);
    private final AtomicLong memoryAnonBytes = new AtomicLong(-1);
    private final AtomicLong memoryFileBytes = new AtomicLong(-1);
    private final AtomicLong cpuUsagePercent = new AtomicLong(-1);
    private final AtomicLong ioReadBytes = new AtomicLong(-1);
    private final AtomicLong ioWriteBytes = new AtomicLong(-1);
    private final AtomicLong filesystemUsedBytes = new AtomicLong(-1);
    private final AtomicLong filesystemFreeBytes = new AtomicLong(-1);

    private volatile long lastCpuUsageMicros = -1;
    private volatile long lastCpuSampleNanos = -1;

    public ContainerResourceMonitor(
            MeterRegistry registry,
            @Value("${broker.storage.dataDir:./data}") String dataDir) {
        this.registry = registry;
        this.cgroupRoot = Path.of("/sys/fs/cgroup");
        this.dataDir = Path.of(dataDir);
    }

    @PostConstruct
    public void init() {
        Gauge.builder("broker.container.memory.current.bytes", memoryCurrentBytes, AtomicLong::get)
                .description("Current cgroup memory usage in bytes")
                .register(registry);
        Gauge.builder("broker.container.memory.anon.bytes", memoryAnonBytes, AtomicLong::get)
                .description("Current cgroup anonymous memory in bytes")
                .register(registry);
        Gauge.builder("broker.container.memory.file.bytes", memoryFileBytes, AtomicLong::get)
                .description("Current cgroup file cache memory in bytes")
                .register(registry);
        Gauge.builder("broker.container.cpu.usage.percent", cpuUsagePercent, AtomicLong::get)
                .description("Approximate container CPU usage percent based on cgroup cpu.stat")
                .register(registry);
        Gauge.builder("broker.container.io.read.bytes", ioReadBytes, AtomicLong::get)
                .description("Cumulative container block IO read bytes from cgroup io.stat")
                .register(registry);
        Gauge.builder("broker.container.io.write.bytes", ioWriteBytes, AtomicLong::get)
                .description("Cumulative container block IO write bytes from cgroup io.stat")
                .register(registry);
        Gauge.builder("broker.container.filesystem.used.bytes", filesystemUsedBytes, AtomicLong::get)
                .description("Used bytes on the broker data filesystem")
                .register(registry);
        Gauge.builder("broker.container.filesystem.free.bytes", filesystemFreeBytes, AtomicLong::get)
                .description("Free bytes on the broker data filesystem")
                .register(registry);
    }

    @Scheduled(fixedDelay = "10s", initialDelay = "10s")
    public void sample() {
        sampleMemory();
        sampleCpu();
        sampleIo();
        sampleFilesystem();
    }

    private void sampleMemory() {
        memoryCurrentBytes.set(readLong(cgroupRoot.resolve("memory.current")));
        memoryAnonBytes.set(readMemoryStatValue("anon"));
        memoryFileBytes.set(readMemoryStatValue("file"));
    }

    private void sampleCpu() {
        long usageMicros = readCpuUsageMicros();
        long now = System.nanoTime();
        if (usageMicros < 0) {
            cpuUsagePercent.set(-1);
            return;
        }
        if (lastCpuUsageMicros >= 0 && lastCpuSampleNanos > 0) {
            long usageDeltaMicros = usageMicros - lastCpuUsageMicros;
            long wallDeltaNanos = now - lastCpuSampleNanos;
            if (usageDeltaMicros >= 0 && wallDeltaNanos > 0) {
                double percent = (usageDeltaMicros * 1_000.0 / wallDeltaNanos) * 100.0;
                cpuUsagePercent.set(Math.max(0L, Math.round(percent)));
            }
        }
        lastCpuUsageMicros = usageMicros;
        lastCpuSampleNanos = now;
    }

    private void sampleIo() {
        long readBytes = 0;
        long writeBytes = 0;
        Path ioStat = cgroupRoot.resolve("io.stat");
        if (!Files.exists(ioStat)) {
            ioReadBytes.set(-1);
            ioWriteBytes.set(-1);
            return;
        }
        try {
            for (String line : Files.readAllLines(ioStat)) {
                String[] parts = line.trim().split("\\s+");
                for (String part : parts) {
                    if (part.startsWith("rbytes=")) {
                        readBytes += parseLong(part.substring("rbytes=".length()));
                    } else if (part.startsWith("wbytes=")) {
                        writeBytes += parseLong(part.substring("wbytes=".length()));
                    }
                }
            }
            ioReadBytes.set(readBytes);
            ioWriteBytes.set(writeBytes);
        } catch (IOException e) {
            log.debug("Could not read cgroup io stats: {}", e.getMessage());
        }
    }

    private void sampleFilesystem() {
        try {
            Path path = Files.exists(dataDir) ? dataDir : dataDir.toAbsolutePath();
            FileStore fileStore = Files.getFileStore(path);
            long total = fileStore.getTotalSpace();
            long free = fileStore.getUsableSpace();
            filesystemFreeBytes.set(free);
            filesystemUsedBytes.set(Math.max(0L, total - free));
        } catch (IOException e) {
            log.debug("Could not read filesystem stats for {}: {}", dataDir, e.getMessage());
        }
    }

    private long readMemoryStatValue(String key) {
        Path statPath = cgroupRoot.resolve("memory.stat");
        if (!Files.exists(statPath)) {
            return -1;
        }
        try {
            List<String> lines = Files.readAllLines(statPath);
            for (String line : lines) {
                String[] parts = line.trim().split("\\s+");
                if (parts.length == 2 && key.equals(parts[0])) {
                    return parseLong(parts[1]);
                }
            }
        } catch (IOException e) {
            log.debug("Could not read memory.stat: {}", e.getMessage());
        }
        return -1;
    }

    private long readCpuUsageMicros() {
        Path cpuStat = cgroupRoot.resolve("cpu.stat");
        if (!Files.exists(cpuStat)) {
            return -1;
        }
        try {
            for (String line : Files.readAllLines(cpuStat)) {
                String[] parts = line.trim().split("\\s+");
                if (parts.length == 2 && "usage_usec".equals(parts[0])) {
                    return parseLong(parts[1]);
                }
            }
        } catch (IOException e) {
            log.debug("Could not read cpu.stat: {}", e.getMessage());
        }
        return -1;
    }

    private long readLong(Path path) {
        try {
            if (!Files.exists(path)) {
                return -1;
            }
            return parseLong(Files.readString(path).trim());
        } catch (IOException e) {
            log.debug("Could not read {}: {}", path, e.getMessage());
            return -1;
        }
    }

    private long parseLong(String value) {
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            return -1;
        }
    }
}
