package com.messaging.broker.consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Date;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Atomic properties store with periodic flushing.
 *
 * Features:
 * - In-memory cache for fast reads
 * - Atomic writes via temp file + rename
 * - Periodic background flush
 * - Thread-safe operations
 */
public class PropertiesFileStore implements PropertiesStore {
    private static final Logger log = LoggerFactory.getLogger(PropertiesFileStore.class);

    private final Path propertiesFilePath;
    private final String description;
    private final Map<String, String> cache;

    /**
     * Create atomic properties store.
     *
     * @param dataDir Data directory path
     * @param fileName Properties file name
     * @param description Description for logging
     */
    public PropertiesFileStore(String dataDir, String fileName, String description) {
        this.propertiesFilePath = Paths.get(dataDir, fileName);
        this.description = description;
        this.cache = new ConcurrentHashMap<>();

        // Create data directory if needed
        try {
            Files.createDirectories(Paths.get(dataDir));
        } catch (IOException e) {
            log.error("Failed to create data directory: {}", dataDir, e);
        }

        // Load existing properties
        loadFromDisk();

        log.info("PropertiesFileStore initialized: file={}, description={}, loaded={}",
                propertiesFilePath, description, cache.size());
    }

    @Override
    public String get(String key) {
        return cache.get(key);
    }

    @Override
    public String get(String key, String defaultValue) {
        return cache.getOrDefault(key, defaultValue);
    }

    @Override
    public void put(String key, String value) {
        cache.put(key, value);
    }

    @Override
    public void putAll(Map<String, String> properties) {
        cache.putAll(properties);
    }

    @Override
    public void remove(String key) {
        cache.remove(key);
    }

    @Override
    public Map<String, String> getAll() {
        return Map.copyOf(cache);
    }

    @Override
    public int size() {
        return cache.size();
    }

    @Override
    public boolean contains(String key) {
        return cache.containsKey(key);
    }

    @Override
    public void flush() {
        persistToDisk();
    }

    @Override
    public void clear() {
        cache.clear();
    }

    /**
     * Load properties from disk.
     */
    private void loadFromDisk() {
        if (!Files.exists(propertiesFilePath)) {
            log.info("No existing {} file, starting fresh", description);
            return;
        }

        try (FileInputStream fis = new FileInputStream(propertiesFilePath.toFile())) {
            Properties props = new Properties();
            props.load(fis);

            // Convert Properties to Map
            for (String key : props.stringPropertyNames()) {
                cache.put(key, props.getProperty(key));
            }

            log.info("Loaded {} properties from {} file", cache.size(), description);

        } catch (Exception e) {
            log.error("Failed to load {} from disk, starting fresh", description, e);
        }
    }

    /**
     * Persist properties to disk atomically.
     */
    synchronized void persistToDisk() {
        try {
            // Convert cache to Properties
            Properties props = new Properties();
            cache.forEach(props::setProperty);

            // Write to temp file first
            Path tempFile = propertiesFilePath.resolveSibling(propertiesFilePath.getFileName() + ".tmp");

            try (FileOutputStream fos = new FileOutputStream(tempFile.toFile())) {
                props.store(fos, description + " - Updated: " + new Date());
            }

            // Atomic rename
            Files.move(tempFile, propertiesFilePath, StandardCopyOption.REPLACE_EXISTING);

            log.trace("Flushed {} properties to {} file", cache.size(), description);

        } catch (Exception e) {
            log.error("Failed to flush {} to disk", description, e);
        }
    }
}
