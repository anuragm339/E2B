package com.messaging.storage.mmap;

import com.messaging.common.model.EventType;
import com.messaging.common.model.MessageRecord;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class MMapStorageEngineTest {

    private Path tempDir;
    private MMapStorageEngine storage;

    @BeforeEach
    void setUp() throws IOException {
        tempDir = Files.createTempDirectory("storage-test");
        storage = new MMapStorageEngine(tempDir.toString(), 1024 * 1024); // 1MB segments
        storage.recover();
    }

    @AfterEach
    void tearDown() throws IOException {
        if (storage != null) {
            storage.close();
        }

        // Clean up temp directory
        if (tempDir != null && Files.exists(tempDir)) {
            Files.walk(tempDir)
                    .sorted(Comparator.reverseOrder())
                    .forEach(path -> {
                        try {
                            Files.delete(path);
                        } catch (IOException e) {
                            // Ignore
                        }
                    });
        }
    }

    @Test
    void testAppendAndRead() {
        // Create a MESSAGE event
        MessageRecord record = new MessageRecord(
                "product_123",
                EventType.MESSAGE,
                "{\"name\":\"Widget\",\"price\":9.99}",
                Instant.now()
        );

        // Append
        long offset = storage.append("price-topic", 0, record);
        assertEquals(0, offset);

        // Read back
        List<MessageRecord> records = storage.read("price-topic", 0, 0, 10);
        assertEquals(1, records.size());

        MessageRecord read = records.get(0);
        assertEquals("product_123", read.getMsgKey());
        assertEquals(EventType.MESSAGE, read.getEventType());
        assertEquals("{\"name\":\"Widget\",\"price\":9.99}", read.getData());
    }

    @Test
    void testMultipleMessages() {
        // Append multiple messages
        for (int i = 0; i < 100; i++) {
            MessageRecord record = new MessageRecord(
                    "product_" + i,
                    EventType.MESSAGE,
                    "{\"id\":" + i + "}",
                    Instant.now()
            );

            long offset = storage.append("test-topic", 0, record);
            assertEquals(i, offset);
        }

        // Read all
        List<MessageRecord> records = storage.read("test-topic", 0, 0, 100);
        assertEquals(100, records.size());

        // Verify order
        for (int i = 0; i < 100; i++) {
            assertEquals("product_" + i, records.get(i).getMsgKey());
        }
    }

    @Test
    void testDeleteEvent() {
        // Append a DELETE event
        MessageRecord deleteRecord = new MessageRecord(
                "product_456",
                EventType.DELETE,
                null, // DELETE events have null data
                Instant.now()
        );

        long offset = storage.append("price-topic", 0, deleteRecord);
        assertEquals(0, offset);

        // Read back
        List<MessageRecord> records = storage.read("price-topic", 0, 0, 10);
        assertEquals(1, records.size());

        MessageRecord read = records.get(0);
        assertEquals("product_456", read.getMsgKey());
        assertEquals(EventType.DELETE, read.getEventType());
        assertNull(read.getData());
    }

    @Test
    void testReadFromMiddle() {
        // Append 50 messages
        for (int i = 0; i < 50; i++) {
            MessageRecord record = new MessageRecord(
                    "key_" + i,
                    EventType.MESSAGE,
                    "data_" + i,
                    Instant.now()
            );
            storage.append("test-topic", 0, record);
        }

        // Read from offset 25
        List<MessageRecord> records = storage.read("test-topic", 0, 25, 10);
        assertEquals(10, records.size());
        assertEquals("key_25", records.get(0).getMsgKey());
        assertEquals("key_34", records.get(9).getMsgKey());
    }

    @Test
    void testGetCurrentOffset() {
        assertEquals(-1, storage.getCurrentOffset("empty-topic", 0));

        storage.append("test-topic", 0, createTestRecord("key1"));
        assertEquals(0, storage.getCurrentOffset("test-topic", 0));

        storage.append("test-topic", 0, createTestRecord("key2"));
        assertEquals(1, storage.getCurrentOffset("test-topic", 0));

        storage.append("test-topic", 0, createTestRecord("key3"));
        assertEquals(2, storage.getCurrentOffset("test-topic", 0));
    }

    @Test
    void testRecovery() throws IOException {
        // Append some data
        for (int i = 0; i < 10; i++) {
            storage.append("recovery-topic", 0, createTestRecord("key_" + i));
        }

        long currentOffset = storage.getCurrentOffset("recovery-topic", 0);
        assertEquals(9, currentOffset);

        // Close and reopen
        storage.close();
        storage = new MMapStorageEngine(tempDir.toString(), 1024 * 1024);
        storage.recover();

        // Verify data is still there
        long recoveredOffset = storage.getCurrentOffset("recovery-topic", 0);
        assertEquals(9, recoveredOffset);

        List<MessageRecord> records = storage.read("recovery-topic", 0, 0, 10);
        assertEquals(10, records.size());
    }

    private MessageRecord createTestRecord(String key) {
        return new MessageRecord(
                key,
                EventType.MESSAGE,
                "{\"test\":\"data\"}",
                Instant.now()
        );
    }
}
