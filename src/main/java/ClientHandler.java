
import java.io.BufferedInputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class ClientHandler implements Runnable {
    private final Socket clientSocket;
    
    // API Key constants
    private static final short API_VERSIONS = 18;
    private static final short DESCRIBE_TOPIC_PARTITIONS = 75;
    
    // Error codes
    private static final short UNSUPPORTED_VERSION_ERROR = 35;
    private static final short NO_ERROR = 0;
    private static final short UNKNOWN_TOPIC_OR_PARTITION = 3;
    

    public ClientHandler(Socket clientSocket) {
        this.clientSocket = clientSocket;
    }

    @Override
    public void run() {
        System.err.println("Handling client: " + clientSocket.getRemoteSocketAddress());

        try {
            // Handle multiple requests from the same client
            while (!clientSocket.isClosed() && clientSocket.isConnected()) {
                try {
                    handleSingleRequest();
                } catch (IOException e) {
                    System.err.println("Connection error with client "
                            + clientSocket.getRemoteSocketAddress() + ": " + e.getMessage());
                    break;
                }
            }
        } finally {
            closeConnection();
        }
    }

    private void handleSingleRequest() throws IOException {
        BufferedInputStream in = new BufferedInputStream(clientSocket.getInputStream());

        // Read message size (4 bytes)
        byte[] messageSizeBytes = in.readNBytes(4);
        if (messageSizeBytes.length < 4) {
            throw new IOException("Connection closed by client or incomplete message size");
        }
        int messageSize = ByteBuffer.wrap(messageSizeBytes).getInt();

        // Read the full message
        byte[] fullMessage = in.readNBytes(messageSize);
        if (fullMessage.length < messageSize) {
            throw new IOException("Connection closed by client or incomplete message");
        }
        ByteBuffer messageBuffer = ByteBuffer.wrap(fullMessage);

        // Parse the message
        byte[] apiKey = new byte[2];
        byte[] apiVersion = new byte[2];
        messageBuffer.get(apiKey);
        messageBuffer.get(apiVersion);
        short apiKeyValue = ByteBuffer.wrap(apiKey).getShort();
        short apiVersionValue = ByteBuffer.wrap(apiVersion).getShort();
        int correlationId = messageBuffer.getInt();
        byte[] correlationIdBytes = ByteBuffer.allocate(4).putInt(correlationId).array();

        System.err.println("Client " + clientSocket.getRemoteSocketAddress()
                + " - API Key: " + apiKeyValue + ", Version: " + apiVersionValue
                + ", Correlation ID: " + correlationId);

        // Different APIs have different request formats
        if (apiKeyValue == API_VERSIONS) {
            // APIVersions has client_id (nullable string, 2 bytes length) and tagged fields
            System.err.println("Buffer position before client ID: " + messageBuffer.position());
            
            byte[] clientIdLengthBytes = new byte[2];
            messageBuffer.get(clientIdLengthBytes);
            short clientIdLength = ByteBuffer.wrap(clientIdLengthBytes).getShort();
            System.err.println("Client ID length: " + clientIdLength);
            
            if (clientIdLength > 0) {
                byte[] clientIdBytes = new byte[clientIdLength];
                messageBuffer.get(clientIdBytes);
                String clientId = new String(clientIdBytes);
                System.err.println("Client ID: " + clientId);
            }
            
            byte taggedBufferBytes = messageBuffer.get();
            System.err.println("Tagged buffer byte: " + taggedBufferBytes);
        } else if (apiKeyValue == DESCRIBE_TOPIC_PARTITIONS) {
            // DescribeTopicPartitions has client_id (nullable string, 2 bytes length) and tagged fields
            System.err.println("Buffer position before client ID: " + messageBuffer.position());
            
            // Read client_id (nullable string with 2-byte length)
            byte[] clientIdLengthBytes = new byte[2];
            messageBuffer.get(clientIdLengthBytes);
            short clientIdLength = ByteBuffer.wrap(clientIdLengthBytes).getShort();
            System.err.println("Client ID length: " + clientIdLength);
            
            if (clientIdLength > 0) {
                byte[] clientIdBytes = new byte[clientIdLength];
                messageBuffer.get(clientIdBytes);
                String clientId = new String(clientIdBytes);
                System.err.println("Client ID: " + clientId);
            }
            
            // Skip tagged fields
            byte taggedFields = messageBuffer.get();
            System.err.println("Tagged fields: " + taggedFields);
        }
        
        // Debug: Show current buffer position before parsing topics
        System.err.println("Buffer position before API routing: " + messageBuffer.position());
        System.err.println("Remaining bytes in buffer: " + messageBuffer.remaining());

        // Routing to different handlers based on API key
        switch (apiKeyValue) {
            case API_VERSIONS -> handleAPIVersions(correlationIdBytes, apiVersionValue);
                
            case DESCRIBE_TOPIC_PARTITIONS -> handleDescribeTopicPartitions(correlationIdBytes, apiVersionValue, messageBuffer);
                
            default -> System.err.println("Error: Unsupported API key: " + apiKeyValue);
        }
    }

    private void handleAPIVersions(byte[] correlationIdBytes, short apiVersionValue) throws IOException {
        if (apiVersionValue > 4 || apiVersionValue < 0) {
            sendErrorResponse(correlationIdBytes);
        } else {
            sendAPIVersionsResponse(correlationIdBytes);
        }
    }

    private void handleDescribeTopicPartitions(byte[] correlationIdBytes, short apiVersionValue, ByteBuffer messageBuffer) throws IOException {
        if (apiVersionValue != 0) {
            sendErrorResponse(correlationIdBytes);
        } else {
            // Parse topics from request
            List<String> requestedTopics = parseTopicsFromRequest(messageBuffer);
            sendDescribeTopicPartitionsResponse(correlationIdBytes, requestedTopics);
        }
    }

    private List<String> parseTopicsFromRequest(ByteBuffer messageBuffer) {
        List<String> topicArray = new ArrayList<>();
        
        System.err.println("Buffer position at start of parseTopics: " + messageBuffer.position());
        System.err.println("Remaining bytes: " + messageBuffer.remaining());
        
        // Read topics array length (compact array format)
        byte arrayLength = messageBuffer.get();
        System.err.println("Raw array length byte: " + arrayLength + " (as unsigned: " + (arrayLength & 0xFF) + ")");
        int numTopics = arrayLength - 1; // Compact array: actual count = value - 1
        
        System.err.println("Parsing " + numTopics + " topics from request");
        
        if (numTopics < 0) {
            System.err.println("ERROR: Negative topic count, something is wrong with parsing!");
            return topicArray;
        }
        
        for (int i = 0; i < numTopics; i++) {
            // Read topic name length (compact string format)
            byte topicNameLengthByte = messageBuffer.get();
            int topicNameLength = topicNameLengthByte - 1; // Compact string: actual length = value - 1
            
            System.err.println("Topic " + i + " - length byte: " + topicNameLengthByte + ", actual length: " + topicNameLength);
            
            // Read topic name bytes
            byte[] topicNameBytes = new byte[topicNameLength];
            messageBuffer.get(topicNameBytes);
            
            String topicName = new String(topicNameBytes);
            topicArray.add(topicName);
            System.err.println("Parsed topic name: " + topicName);
            
            // Read response_partition_limit (4 bytes)
            int responsePartitionLimit = messageBuffer.getInt();
            System.err.println("Response partition limit: " + responsePartitionLimit);
            
            // Skip tagged fields for this topic
            byte topicTaggedFields = messageBuffer.get();
            System.err.println("Topic tagged fields: " + topicTaggedFields);
        }
        
        // Read cursor (1 byte)
        byte cursor = messageBuffer.get();
        System.err.println("Cursor: " + cursor);
        
        // Skip final tagged fields
        byte finalTaggedFields = messageBuffer.get();
        System.err.println("Final tagged fields: " + finalTaggedFields);
        
        System.err.println("Final parsed topics: " + topicArray);
        return topicArray;
    }

    private void sendErrorResponse(byte[] correlationIdBytes) throws IOException {
        System.err.println("Sending error response to client: " + clientSocket.getRemoteSocketAddress());

        byte[] lengthBytes = ByteBuffer.allocate(4).putInt(7).array();
        byte[] errorCodeBytes = ByteBuffer.allocate(2).putShort(UNSUPPORTED_VERSION_ERROR).array();
        byte[] emptyTaggedBuffer = new byte[]{0};

        writeToClient(lengthBytes, correlationIdBytes, errorCodeBytes, emptyTaggedBuffer);
    }

    private void sendAPIVersionsResponse(byte[] correlationIdBytes) throws IOException {
        System.err.println("Sending APIVersions response to client: " + clientSocket.getRemoteSocketAddress());

        // Calculate response components
        byte[] lengthBytes = ByteBuffer.allocate(4).putInt(26).array();
        byte[] errorCodeBytes = ByteBuffer.allocate(2).putShort(NO_ERROR).array();
        byte[] numApiKeysBytes = new byte[]{3}; // Compact array format (2 entries + 1)

        // API 18 (APIVersions)
        byte[] api18Data = buildApiKeyData(API_VERSIONS, (short) 0, (short) 4);
        
        // API 75 (DescribeTopicPartitions)
        byte[] api75Data = buildApiKeyData(DESCRIBE_TOPIC_PARTITIONS, (short) 0, (short) 0);

        byte[] throttleTimeBytes = ByteBuffer.allocate(4).putInt(0).array();
        byte[] taggedBuffer = new byte[]{0};

        // Write complete response
        writeToClient(lengthBytes, correlationIdBytes, errorCodeBytes, numApiKeysBytes, 
                     api18Data, api75Data, throttleTimeBytes, taggedBuffer);
        
        System.err.println("APIVersions response sent successfully to client: " + clientSocket.getRemoteSocketAddress());
    }

    private byte[] buildApiKeyData(short apiKey, short minVersion, short maxVersion) {
        ByteBuffer buffer = ByteBuffer.allocate(7); // 2+2+2+1 bytes
        buffer.putShort(apiKey);
        buffer.putShort(minVersion);
        buffer.putShort(maxVersion);
        buffer.put((byte) 0); // tagged fields
        return buffer.array();
    }

    // here we are implementing the response using v0 response header.
    private void sendDescribeTopicPartitionsResponse(byte[] correlationIdBytes, List<String> requestedTopics) throws IOException {
        System.err.println("Sending DescribeTopicPartitions response to: " + clientSocket.getRemoteSocketAddress());
        
        // Build response for each topic
        ByteBuffer responseBuffer = ByteBuffer.allocate(1024); // Allocate enough space
        
        // Response body (no response header error code for v0)
        responseBuffer.putInt(0); // Throttle time (4 bytes)
        
        // Topics array (compact format)
        responseBuffer.put((byte) (requestedTopics.size() + 1)); // Compact array length
        
        for (String topicName : requestedTopics) {
            // Topic error code
            responseBuffer.putShort(UNKNOWN_TOPIC_OR_PARTITION); // Error code 3
            
            // Topic name (compact string)
            byte[] topicNameBytes = topicName.getBytes();
            responseBuffer.put((byte) (topicNameBytes.length + 1)); // Compact string length
            responseBuffer.put(topicNameBytes);
            
            // Topic ID (16 bytes of zeros for null UUID)
            responseBuffer.put(new byte[16]);
            
            // Is internal (1 byte)
            responseBuffer.put((byte) 0); // false
            
            // Partitions array (empty, compact format)
            responseBuffer.put((byte) 1); // Empty compact array (1 means 0 elements)
            
            // Topic authorized operations (4 bytes)
            responseBuffer.putInt(0x00000df8);
            
            // Tagged fields for topic
            responseBuffer.put((byte) 0);
        }
        
        // Next cursor (null cursor in compact format)
        // For a null cursor, we need to send: 0xff (null marker) 
        responseBuffer.put((byte) 0xff);
        
        // Final tagged fields
        responseBuffer.put((byte) 0);
        
        // Calculate actual response size
        int responseSize = responseBuffer.position();
        byte[] responseData = new byte[responseSize];
        responseBuffer.flip();
        responseBuffer.get(responseData);
        
        // Total message length = correlation ID (4) + header tagged fields (1) + response data
        int totalLength = 4 + 1 + responseSize;
        byte[] lengthBytes = ByteBuffer.allocate(4).putInt(totalLength).array();
        
        // Send complete response
        writeToClient(lengthBytes, correlationIdBytes, new byte[]{0}, responseData);
        
        System.err.println("DescribeTopicPartitions response sent successfully for topics: " + requestedTopics);
        System.err.println("Total response length: " + totalLength + " bytes");
    }

    private void writeToClient(byte[]... dataArrays) throws IOException {
        for (byte[] data : dataArrays) {
            clientSocket.getOutputStream().write(data);
        }
        clientSocket.getOutputStream().flush();
    }

    private void closeConnection() {
        try {
            if (clientSocket != null && !clientSocket.isClosed()) {
                clientSocket.close();
                System.err.println("Connection closed for client: " + clientSocket.getRemoteSocketAddress());
            }
        } catch (IOException e) {
            System.err.println("Error closing connection: " + e.getMessage());
        }
    }
}
