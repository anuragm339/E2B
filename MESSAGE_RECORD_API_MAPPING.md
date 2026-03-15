# MessageRecord API Response Mapping

## Changes Made

### 1. Updated MessageRecord.java to Support External API Format

**API Response Format:**
```json
{
  "type": "colleague-facts-jobs",
  "key": "e7579634-5997-46f5-b680-1d2855c96dee",
  "contentType": "application/json",
  "offset": "113800005",
  "created": "2020-09-15T12:01:17.230402Z",
  "data": "[{\"locationUUID\":\"bfdd6e71-c068-468a-83b6-d109e4470d7c\"}]"
}
```

**DELETE Event (no data field):**
```json
{
  "type": "colleague-facts-jobs",
  "key": "abc123-deleted",
  "contentType": "application/json",
  "offset": "113800006",
  "created": "2020-09-15T12:02:00.000000Z"
}
```

### 2. Field Mapping

| API Field | MessageRecord Field | Type Conversion | Notes |
|-----------|-------------------|-----------------|-------|
| `type` | `topic` | Direct | Maps using @JsonProperty("type") |
| `key` | `msgKey` | Direct | Maps using @JsonProperty("key") |
| `offset` | `offset` | String → long | Custom setter handles both |
| `created` | `createdAt` | String → Instant | Jackson auto-converts with jsr310 module |
| `data` | `data` | Direct | Null/missing = DELETE event |
| `contentType` | `contentType` | Direct | New field added |

### 3. Auto-Detection of DELETE Events

**Logic in `setData()`:**
```java
public void setData(String data) {
    this.data = data;
    // Auto-detect: if data is null/empty → DELETE event
    if (this.eventType == null) {
        this.eventType = (data == null || data.isEmpty())
            ? EventType.DELETE
            : EventType.MESSAGE;
    }
}
```

**Result:**
- If API response has `data` field with content → `EventType.MESSAGE`
- If API response has NO `data` field or null → `EventType.DELETE`

### 4. String Offset Handling

**Custom setter handles both String and Number:**
```java
@JsonProperty("offset")
public void setOffset(Object offset) {
    if (offset instanceof String) {
        this.offset = Long.parseLong((String) offset);
    } else if (offset instanceof Number) {
        this.offset = ((Number) offset).longValue();
    }
}
```

### 5. Dependencies Added

**common/build.gradle:**
```gradle
// Jackson for JSON serialization/deserialization
api 'com.fasterxml.jackson.core:jackson-annotations:2.15.2'
api 'com.fasterxml.jackson.core:jackson-databind:2.15.2'
api 'com.fasterxml.jackson.datatype:jackson-datatype-jsr310:2.15.2'
```

## Usage Example

### Deserializing API Response

```java
ObjectMapper mapper = new ObjectMapper();
mapper.registerModule(new JavaTimeModule());  // For Instant support

// Single record
MessageRecord record = mapper.readValue(jsonString, MessageRecord.class);

// Array of records
MessageRecord[] records = mapper.readValue(jsonArray, MessageRecord[].class);
```

### Checking Event Type

```java
if (record.getEventType() == EventType.MESSAGE) {
    // Process regular message
    String data = record.getData();
    processMessage(data);
} else if (record.getEventType() == EventType.DELETE) {
    // Handle delete event
    String key = record.getMsgKey();
    deleteByKey(key);
}
```

## Verification

### Test Case 1: MESSAGE Event
**Input:**
```json
{
  "type": "test-topic",
  "key": "msg-001",
  "offset": "12345",
  "created": "2024-01-01T00:00:00Z",
  "data": "{\"value\":\"test\"}"
}
```

**Expected:**
- `record.getTopic()` → "test-topic"
- `record.getMsgKey()` → "msg-001"
- `record.getOffset()` → 12345L
- `record.getEventType()` → EventType.MESSAGE
- `record.getData()` → "{\"value\":\"test\"}"

### Test Case 2: DELETE Event
**Input:**
```json
{
  "type": "test-topic",
  "key": "msg-002",
  "offset": "12346",
  "created": "2024-01-01T00:01:00Z"
}
```

**Expected:**
- `record.getTopic()` → "test-topic"
- `record.getMsgKey()` → "msg-002"
- `record.getOffset()` → 12346L
- `record.getEventType()` → EventType.DELETE
- `record.getData()` → null

## Benefits

1. **Direct Compatibility** - API responses can be directly deserialized
2. **Auto-Detection** - DELETE events automatically detected (no manual flag needed)
3. **Flexible Offset** - Handles both String and numeric offset formats
4. **Type Safety** - Jackson ensures type conversion safety
5. **No Manual Parsing** - No need to manually parse and map fields

## Migration Impact

### Existing Code
All existing code that creates MessageRecord programmatically continues to work:
```java
MessageRecord msg = new MessageRecord(
    offset, topic, partition, msgKey,
    EventType.MESSAGE, data, createdAt
);
```

### New Code (API Integration)
```java
// HttpPipeConnector streaming parser
MessageRecord record = objectMapper.readValue(parser, MessageRecord.class);
// Auto-mapped and event type auto-detected!
```

## Notes

- The `partition` field defaults to 0 (single partition per topic)
- The `crc32` field is calculated separately during storage
- `contentType` is informational only (broker doesn't validate it)
- Instant fields use ISO-8601 format (handled by jackson-datatype-jsr310)
