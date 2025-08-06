
import java.io.BufferedInputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;

public class ClientHandler implements Runnable {
    private final Socket clientSocket;
    
    // API Key constants
    private static final short API_VERSIONS = 18;
    private static final short DESCRIBE_TOPIC_PARTITIONS = 75;
    
    // Error codes
    private static final short UNSUPPORTED_VERSION_ERROR = 35;
    private static final short NO_ERROR = 0;

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

        // Route to different handlers based on API key
        switch (apiKeyValue) {
            case API_VERSIONS:
                handleAPIVersions(correlationIdBytes, apiVersionValue);
                break;
                
            case DESCRIBE_TOPIC_PARTITIONS:
                handleDescribeTopicPartitions(correlationIdBytes, apiVersionValue, messageBuffer);
                break;
                
            default:
                System.err.println("Error: Unsupported API key: " + apiKeyValue);
                break;
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
            sendDescribeTopicPartitionsResponse(correlationIdBytes);
        }
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

    private void sendDescribeTopicPartitionsResponse(byte[] correlationIdBytes) throws IOException {
        System.err.println("Sending DescribeTopicPartitions response to: " + clientSocket.getRemoteSocketAddress());
        
        // Simple empty response
        byte[] lengthBytes = ByteBuffer.allocate(4).putInt(13).array();
        byte[] errorCodeBytes = ByteBuffer.allocate(2).putShort(NO_ERROR).array();
        byte[] throttleTimeBytes = ByteBuffer.allocate(4).putInt(0).array();
        byte[] topicsArrayBytes = new byte[]{1}; // Empty compact array (1 means 0 elements)
        byte[] nextCursorBytes = new byte[]{0}; // Null cursor
        byte[] taggedFieldsBytes = new byte[]{0}; // No tagged fields
        
        writeToClient(lengthBytes, correlationIdBytes, errorCodeBytes, throttleTimeBytes, 
                     topicsArrayBytes, nextCursorBytes, taggedFieldsBytes);
        
        System.err.println("DescribeTopicPartitions response sent successfully");
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
