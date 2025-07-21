# Kafka APIVersions Implementation - Complete Breakdown

## Overview

This document explains in absolute detail what happens when implementing a basic Kafka server that handles APIVersions requests. We'll break down every single byte, every protocol detail, and every step of the process.

## What is the APIVersions Request?

The APIVersions request (API key 18) is the first request that Kafka clients send to discover what API versions a broker supports. It's like asking "Hey server, what can you do?"

## The Kafka Wire Protocol Structure

### Basic Message Format
Every Kafka message follows this format:
```
[message_length: 4 bytes] [message_payload: variable length]
```

The `message_length` field tells us how many bytes follow it.

### Request Format
```
[message_length: INT32] [api_key: INT16] [api_version: INT16] [correlation_id: INT32] [request_body: variable]
```

### Response Format  
```
[message_length: INT32] [correlation_id: INT32] [response_body: variable]
```

## Step-by-Step Breakdown

### 1. Server Setup
```java
ServerSocket serverSocket = new ServerSocket(9092);
serverSocket.setReuseAddress(true);
clientSocket = serverSocket.accept();
```

- **Port 9092**: Standard Kafka port
- **setReuseAddress(true)**: Allows immediate port reuse after program stops
- **accept()**: Blocks until a client connects

### 2. Waiting for Data
```java
while (clientSocket.getInputStream().available() == 0) {
    Thread.sleep(1000);
}
```

This polls every second until data arrives. Not the most elegant, but works for a simple implementation.

### 3. Reading the Complete Request

#### The Problem We Solved
Initially, we were only reading the basic header (12 bytes), but the actual request was 35 bytes! This left unread data in the stream, causing connection issues.

#### The Solution
```java
byte[] messageSizeBytes = in.readNBytes(4);
int messageSize = ByteBuffer.wrap(messageSizeBytes).getInt();
byte[] fullMessage = in.readNBytes(messageSize);
```

**Example Request Hexdump:**
```
00 00 00 23 00 12 00 04 25 ed c0 ae 00 09 6b 61 66 6b 61 2d 63 6c 69 00 0a 6b 61 66 6b 61 2d 63 6c 69 04 30 2e 31 00
```

**Breaking it down:**
- `00 00 00 23` = message size (35 bytes)
- `00 12` = API key (18 = APIVersions)
- `00 04` = API version (4)
- `25 ed c0 ae` = correlation ID (636338350)
- `00 09 6b 61 66 6b 61 2d 63 6c 69` = client ID string "kafka-cli"
- `00 0a 6b 61 66 6b 61 2d 63 6c 69` = client software name "kafka-cli"
- `04 30 2e 31` = client software version "0.1"
- `00` = tagged fields (empty)

### 4. Parsing the Request
```java
ByteBuffer messageBuffer = ByteBuffer.wrap(fullMessage);
byte[] apiKey = new byte[2];
messageBuffer.get(apiKey);
byte[] apiVersion = new byte[2];
messageBuffer.get(apiVersion);
int correlationId = messageBuffer.getInt();

short apiKeyValue = ByteBuffer.wrap(apiKey).getShort();
short apiVersionValue = ByteBuffer.wrap(apiVersion).getShort();
```

We only care about the first few fields for our implementation.

### 5. Validation
```java
if (apiKeyValue != 18) {
    System.err.println("Error: Expected API key 18 (APIVersions), got: " + apiKeyValue);
    return;
}

if (apiVersionValue > 4 || apiVersionValue < 0) {
    // Send error response with code 35 (UNSUPPORTED_VERSION)
}
```

We only support APIVersions requests (key 18) with versions 0-4.

## The Response Format

### APIVersions Response Structure
```
ResponseHeader:
  correlation_id (INT32)
ResponseBody:
  error_code (INT16)
  num_api_keys (INT8)  
  ApiKeys[]:
    api_key (INT16)
    min_version (INT16)
    max_version (INT16)
  TAG_BUFFER (byte)
  throttle_time_ms (INT32)
  TAG_BUFFER (byte)
```

### The Array Length Encoding Issue

This was the trickiest part! In Kafka protocol, array lengths use "compact array" encoding:
- `0` = null array
- `1` = empty array (0 elements)
- `2` = 1 element
- `n+1` = n elements

**So for 1 API key, we send `num_api_keys = 2`**

### Success Response Bytes Breakdown

**Our Response:**
```java
byte[] lengthBytes = ByteBuffer.allocate(4).putInt(19).array();           // [00 00 00 13]
byte[] correlationBytes = ByteBuffer.allocate(4).putInt(correlationId).array(); // [25 ed c0 ae]
byte[] errorCodeBytes = ByteBuffer.allocate(2).putShort((short) 0).array();     // [00 00]
byte[] numApiKeysBytes = new byte[]{2};                                   // [02]
byte[] apiKeyBytes = ByteBuffer.allocate(2).putShort((short) 18).array();       // [00 12]
byte[] minVersionBytes = ByteBuffer.allocate(2).putShort((short) 0).array();    // [00 00]
byte[] maxVersionBytes = ByteBuffer.allocate(2).putShort((short) 4).array();    // [00 04]
byte[] taggedBuffer1 = new byte[]{0};                                     // [00]
byte[] throttleTimeBytes = ByteBuffer.allocate(4).putInt(0).array();            // [00 00 00 00]
byte[] taggedBuffer2 = new byte[]{0};                                     // [00]
```

**Hexdump of Final Response:**
```
00 00 00 13 25 ed c0 ae 00 00 02 00 12 00 00 00 04 00 00 00 00 00
```

**What Each Part Means:**
- `00 00 00 13` = Total response length (19 bytes)
- `25 ed c0 ae` = Correlation ID (echoed back)
- `00 00` = Error code (0 = success)
- `02` = Number of API keys (2 means 1 element in compact array)
- `00 12` = API key 18 (APIVersions)
- `00 00` = Min version (0)
- `00 04` = Max version (4)
- `00` = Tagged fields (empty)
- `00 00 00 00` = Throttle time (0 ms)
- `00` = Final tagged fields (empty)

### Error Response (Unsupported Version)

For unsupported API versions (< 0 or > 4):
```java
byte[] lengthBytes = ByteBuffer.allocate(4).putInt(7).array();    // [00 00 00 07]
byte[] correlationBytes = // correlation ID echoed back
byte[] errorCodeBytes = ByteBuffer.allocate(2).putShort((short) 35).array(); // [00 23]
byte[] emptyTaggedBuffer = new byte[]{0};                         // [00]
```

**Error Code 35** = `UNSUPPORTED_VERSION` in Kafka protocol.

## What the Client Learns

When our server sends the successful response, the client learns:
- "This server supports API 18 (APIVersions)"
- "For APIVersions, I can use versions 0, 1, 2, 3, or 4"
- "No errors occurred (error_code = 0)"
- "No throttling is applied (throttle_time_ms = 0)"

## Tagged Fields Explained

Tagged fields are Kafka's way of adding optional fields without breaking compatibility:

### Structure
```
[num_tagged_fields: varint] [tagged_field_1] [tagged_field_2] ...
```

Each tagged field:
```
[tag_id: varint] [length: varint] [data: bytes]
```

### In Our Implementation
We send `00` for empty tagged fields, meaning "0 tagged fields present."

### Why Tagged Fields Matter
- **Backward compatibility**: Older clients ignore unknown tagged fields
- **Forward compatibility**: Newer clients can handle missing tagged fields
- **Protocol evolution**: New features can be added without version bumps

## Common Pitfalls We Avoided

### 1. **Incomplete Request Reading**
❌ **Wrong:** Only reading 12 bytes (basic header)
✅ **Right:** Reading the complete message based on message_length

### 2. **Array Length Encoding**
❌ **Wrong:** `num_api_keys = 1` for 1 element
✅ **Right:** `num_api_keys = 2` for 1 element (compact array encoding)

### 3. **Response Length Calculation**
❌ **Wrong:** Not including all fields in length calculation
✅ **Right:** Carefully counting every byte: 4+2+1+2+2+2+1+4+1 = 19 bytes

### 4. **Connection Management**
❌ **Wrong:** Closing connection immediately after sending
✅ **Right:** Flushing output stream and keeping connection alive briefly

### 5. **Missing Tagged Fields**
❌ **Wrong:** Forgetting tagged field buffers
✅ **Right:** Including empty tagged fields where required

## Testing and Debugging

The tester provides excellent debugging information:

### Hexdumps
Shows exact bytes sent and received for comparison.

### Decoder Output
```
[Decoder] - .ResponseHeader
[Decoder]   - .correlation_id (636338350)
[Decoder] - .ResponseBody
[Decoder]   - .error_code (0)
[Decoder]   - .num_api_keys (1)  # Note: shows logical count, not wire format
[Decoder]   - .ApiKeys[0]
[Decoder]     - .api_key (18)
[Decoder]     - .min_version (0)
[Decoder]     - .max_version (4)
[Decoder]     - .TAG_BUFFER
[Decoder]   - .throttle_time_ms (0)
[Decoder]   - .TAG_BUFFER
```

### Error Messages
When bytes don't match expected format:
```
Error: unexpected 2 bytes remaining in decoder after decoding ApiVersionsResponse
Error: Expected int32 length to be 4 bytes, got 3 bytes
```

These helped us identify the exact issues with our response format.

## Key Takeaways

1. **Read the complete request** - Don't leave unread data in the stream
2. **Understand array encoding** - Kafka uses compact arrays where length = elements + 1
3. **Count bytes carefully** - Every byte matters in the response length
4. **Handle tagged fields** - Even if empty, they're required in the protocol
5. **Test incrementally** - Use hexdumps and decoder output to debug
6. **Follow the spec exactly** - Kafka protocol is very precise about byte layouts

## Real-World Implications

This APIVersions implementation is the foundation for any Kafka broker. Real brokers would:
- Support many more API keys (Produce, Fetch, Metadata, etc.)
- Handle multiple API versions for each key
- Implement proper connection pooling
- Add authentication and authorization
- Optimize for high throughput

But the core protocol handling - reading requests, parsing bytes, constructing responses - remains the same!

## Protocol References

- **API Key 18**: APIVersions
- **Error Code 35**: UNSUPPORTED_VERSION  
- **Compact Arrays**: Length encoding where 0=null, 1=empty, n+1=n elements
- **Tagged Fields**: Optional extensibility mechanism
- **Wire Protocol**: Big-endian byte order, length-prefixed messages

This implementation successfully handles the first step in Kafka client-broker communication!
