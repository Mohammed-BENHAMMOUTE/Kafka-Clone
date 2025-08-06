# Kafka Protocol Implementation Guide

## Overview
This guide explains the Kafka wire protocol messages, their fields, and how to process them in our broker implementation.

## Current Implementation Status
✅ **APIVersions (Key 18)** - Fully implemented  
🔄 **DescribeTopicPartitions (Key 75)** - In progress  
❌ **Fetch (Key 1)** - Not implemented  
❌ **Produce (Key 0)** - Not implemented  

---

## Message Structure

All Kafka messages follow this basic structure:
```
[Message Size (4 bytes)] [API Key (2 bytes)] [API Version (2 bytes)] [Correlation ID (4 bytes)] [Request Body...]
```

### Common Fields
- **Message Size**: Total size of the message (excluding the size field itself)
- **API Key**: Identifies the type of request (see API Keys below)
- **API Version**: Version of the API being used
- **Correlation ID**: Unique identifier to match requests with responses

---

## API Keys Reference

| API Key | Name | Description | Status |
|---------|------|-------------|--------|
| 0 | Produce | Send messages to broker | ❌ Not implemented |
| 1 | Fetch | Retrieve messages from broker | ❌ Not implemented |
| 18 | APIVersions | Get supported API versions | ✅ Implemented |
| 75 | DescribeTopicPartitions | Get topic partition metadata | 🔄 In progress |

---

## 1. APIVersions Request (Key 18)

### Purpose
Allows clients to discover what API versions the broker supports.

### Current Implementation
**Function**: `handleSingleRequest()` + `sendSuccessResponse()` / `sendErrorResponse()`

### Input Message Structure
```
Message Size: 4 bytes
API Key: 2 bytes (value: 18)
API Version: 2 bytes (0-4 supported)
Correlation ID: 4 bytes
Client ID: Variable length string (we don't parse this yet)
Tagged Fields: Variable (we don't parse this yet)
```

### Processing Logic
1. **Validation**: Check if API version is between 0-4
2. **Error Response**: If version > 4 or < 0, send error code 35
3. **Success Response**: If valid, send supported API list

### Response Structure (Success)
```
Message Size: 4 bytes (value: 19)
Correlation ID: 4 bytes (echoed from request)
Error Code: 2 bytes (value: 0 for success)
Number of API Keys: 1 byte (compact array, value: 2 means 1 API key)
  └─ API Key: 2 bytes (value: 18)
  └─ Min Version: 2 bytes (value: 0)
  └─ Max Version: 2 bytes (value: 4)
  └─ Tagged Fields: 1 byte (value: 0)
Throttle Time: 4 bytes (value: 0)
Tagged Fields: 1 byte (value: 0)
```

### Response Structure (Error)
```
Message Size: 4 bytes (value: 7)
Correlation ID: 4 bytes (echoed from request)
Error Code: 2 bytes (value: 35 = UNSUPPORTED_VERSION)
Tagged Fields: 1 byte (value: 0)
```

---

## 2. DescribeTopicPartitions Request (Key 75)

### Purpose
Get metadata about topic partitions (replicas, leaders, etc.)

### Current Status
**Function**: Commented out code `// if (apiKeyValue == 75)`

### Input Message Structure
```
Message Size: 4 bytes
API Key: 2 bytes (value: 75)
API Version: 2 bytes
Correlation ID: 4 bytes
Client ID: Variable length string
Topics Array: Variable
  └─ Topic Name: Variable length string
  └─ Response Partition Limit: 4 bytes
  └─ Response Partition Offset: 4 bytes
  └─ Tagged Fields: Variable
Cursor: Variable (for pagination)
Tagged Fields: Variable
```

### Processing Logic (To Implement)
1. Parse topic names from request
2. Look up partition metadata for each topic
3. Handle unknown topics appropriately
4. Return partition information

### Response Structure (To Implement)
```
Message Size: 4 bytes
Correlation ID: 4 bytes
Throttle Time: 4 bytes
Topics Array: Variable
  └─ Error Code: 2 bytes
  └─ Topic Name: Variable length string
  └─ Topic ID: 16 bytes (UUID)
  └─ Is Internal: 1 byte (boolean)
  └─ Partitions Array: Variable
      └─ Error Code: 2 bytes
      └─ Partition Index: 4 bytes
      └─ Leader ID: 4 bytes
      └─ Leader Epoch: 4 bytes
      └─ Replica Nodes: Array of 4-byte broker IDs
      └─ ISR Nodes: Array of 4-byte broker IDs
      └─ Eligible Leader Replicas: Array of 4-byte broker IDs
      └─ Last Known ELR: Array of 4-byte broker IDs
      └─ Offline Replicas: Array of 4-byte broker IDs
      └─ Tagged Fields: Variable
  └─ Topic Authorized Operations: 4 bytes
  └─ Tagged Fields: Variable
Next Cursor: Variable (for pagination)
Tagged Fields: Variable
```

---

## 3. Fetch Request (Key 1) - To Implement

### Purpose
Retrieve messages from specific topic partitions starting from a given offset.

### Input Message Structure
```
Message Size: 4 bytes
API Key: 2 bytes (value: 1)
API Version: 2 bytes
Correlation ID: 4 bytes
Replica ID: 4 bytes (-1 for consumer clients)
Max Wait Time: 4 bytes (milliseconds to wait for data)
Min Bytes: 4 bytes (minimum bytes to accumulate before response)
Max Bytes: 4 bytes (maximum bytes to return)
Isolation Level: 1 byte (0=READ_UNCOMMITTED, 1=READ_COMMITTED)
Session ID: 4 bytes
Session Epoch: 4 bytes
Topics Array: Variable
  └─ Topic ID: 16 bytes (UUID)
  └─ Partitions Array: Variable
      └─ Partition: 4 bytes
      └─ Current Leader Epoch: 4 bytes
      └─ Fetch Offset: 8 bytes (long)
      └─ Last Fetched Epoch: 4 bytes
      └─ Log Start Offset: 8 bytes (long)
      └─ Partition Max Bytes: 4 bytes
      └─ Tagged Fields: Variable
  └─ Tagged Fields: Variable
Forgotten Topics Array: Variable
Rack ID: Variable length string
Tagged Fields: Variable
```

### Processing Logic (To Implement)
1. Parse topic IDs and partition fetch requests
2. For each partition, read messages from log files starting at fetch offset
3. Respect max wait time, min bytes, and max bytes limits
4. Handle cases where topics/partitions don't exist
5. Return message batches

### Response Structure (To Implement)
```
Message Size: 4 bytes
Correlation ID: 4 bytes
Throttle Time: 4 bytes
Error Code: 2 bytes
Session ID: 4 bytes
Responses Array: Variable
  └─ Topic ID: 16 bytes
  └─ Partitions Array: Variable
      └─ Partition Index: 4 bytes
      └─ Error Code: 2 bytes
      └─ High Watermark: 8 bytes (long)
      └─ Last Stable Offset: 8 bytes (long)
      └─ Log Start Offset: 8 bytes (long)
      └─ Aborted Transactions: Array (can be empty)
      └─ Preferred Read Replica: 4 bytes
      └─ Records: Variable length byte array (message batch)
      └─ Tagged Fields: Variable
  └─ Tagged Fields: Variable
Tagged Fields: Variable
```

---

## 4. Produce Request (Key 0) - To Implement

### Purpose
Send messages to the broker to be stored in topic partitions.

### Input Message Structure
```
Message Size: 4 bytes
API Key: 2 bytes (value: 0)
API Version: 2 bytes
Correlation ID: 4 bytes
Client ID: Variable length string
Transactional ID: Variable length string (nullable)
Acks: 2 bytes (0=no ack, 1=leader ack, -1=all replicas ack)
Timeout: 4 bytes (milliseconds)
Topic Data Array: Variable
  └─ Topic Name: Variable length string
  └─ Partition Data Array: Variable
      └─ Partition Index: 4 bytes
      └─ Records: Variable length byte array (message batch)
      └─ Tagged Fields: Variable
  └─ Tagged Fields: Variable
Tagged Fields: Variable
```

### Processing Logic (To Implement)
1. Parse topic names and partition data
2. Auto-create topics if they don't exist
3. Write message batches to appropriate log files
4. Assign offsets to messages
5. Handle different ack requirements
6. Return success/error responses

### Response Structure (To Implement)
```
Message Size: 4 bytes
Correlation ID: 4 bytes
Responses Array: Variable
  └─ Topic Name: Variable length string
  └─ Partition Responses Array: Variable
      └─ Partition Index: 4 bytes
      └─ Error Code: 2 bytes
      └─ Base Offset: 8 bytes (long, first offset assigned)
      └─ Log Append Time: 8 bytes (long, timestamp)
      └─ Log Start Offset: 8 bytes (long)
      └─ Record Errors: Array (for individual record errors)
      └─ Error Message: Variable length string (nullable)
      └─ Tagged Fields: Variable
  └─ Tagged Fields: Variable
Throttle Time: 4 bytes
Tagged Fields: Variable
```

---

## Implementation Flow

### Current Request Processing (ClientHandler.java)
1. **Read Message Size**: 4 bytes from socket
2. **Read Full Message**: Based on message size
3. **Parse Header**: Extract API Key, Version, Correlation ID
4. **Validate API Key**: Currently only accepts key 18
5. **Process Request**: Based on API key and version
6. **Send Response**: Success or error response

### Next Steps for Multi-API Support
1. **Remove API Key Restriction**: Currently hardcoded to only accept 18
2. **Add API Key Router**: Switch statement to handle different API keys
3. **Implement Message Parsers**: For each API type
4. **Add Response Builders**: For each API type
5. **Add Storage Layer**: For persistent message storage

### Code Structure Recommendations
```java
// Add to ClientHandler.java
private void handleApiRequest(short apiKey, short apiVersion, int correlationId, ByteBuffer messageBuffer) {
    switch (apiKey) {
        case 18: // APIVersions
            handleApiVersions(apiVersion, correlationId);
            break;
        case 75: // DescribeTopicPartitions
            handleDescribeTopicPartitions(apiVersion, correlationId, messageBuffer);
            break;
        case 1: // Fetch
            handleFetch(apiVersion, correlationId, messageBuffer);
            break;
        case 0: // Produce
            handleProduce(apiVersion, correlationId, messageBuffer);
            break;
        default:
            sendUnsupportedApiError(correlationId);
    }
}
```

---

## Testing Commands

Once implemented, test with Kafka command-line tools:

```bash
# Test APIVersions (already working)
kafka-broker-api-versions --bootstrap-server localhost:9092

# Test DescribeTopicPartitions
kafka-topics --bootstrap-server localhost:9092 --describe --topic test-topic

# Test Produce
kafka-console-producer --bootstrap-server localhost:9092 --topic test-topic

# Test Fetch/Consume
kafka-console-consumer --bootstrap-server localhost:9092 --topic test-topic --from-beginning
```
