package com.messaging.broker.consumer;

import java.util.Map;
import java.util.Properties;

/**
 * Repository interface for properties-based persistence.
 *
 * Provides atomic read/write operations with configurable flush behavior.
 */
public interface PropertiesStore {
    /**
     * Get property value by key.
     *
     * @param key Property key
     * @return Property value, or null if not found
     */
    String get(String key);

    /**
     * Get property value by key with default.
     *
     * @param key Property key
     * @param defaultValue Default value if key not found
     * @return Property value, or defaultValue if not found
     */
    String get(String key, String defaultValue);

    /**
     * Put property value.
     *
     * @param key Property key
     * @param value Property value
     */
    void put(String key, String value);

    /**
     * Put all properties from map.
     *
     * @param properties Properties to put
     */
    void putAll(Map<String, String> properties);

    /**
     * Remove property by key.
     *
     * @param key Property key
     */
    void remove(String key);

    /**
     * Get all properties as map.
     *
     * @return All properties
     */
    Map<String, String> getAll();

    /**
     * Get size (number of properties).
     *
     * @return Number of properties
     */
    int size();

    /**
     * Check if key exists.
     *
     * @param key Property key
     * @return true if key exists
     */
    boolean contains(String key);

    /**
     * Flush to disk (immediate write).
     */
    void flush();

    /**
     * Clear all properties.
     */
    void clear();
}
