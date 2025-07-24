import java.io.BufferedInputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;

public class ClientHandler implements Runnable {
    private final Socket clientSocket;

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
                    System.err.println("Connection error with client " + 
                        clientSocket.getRemoteSocketAddress() + ": " + e.getMessage());
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

        System.err.println("Client " + clientSocket.getRemoteSocketAddress() + 
            " - API Key: " + apiKeyValue + ", Version: " + apiVersionValue + 
            ", Correlation ID: " + correlationId);

        // Validate API key
        if (apiKeyValue != 18) {
            System.err.println("Error: Expected API key 18 (APIVersions), got: " + apiKeyValue);
            return;
        }

        // Handle API version validation and response
        if (apiVersionValue > 4 || apiVersionValue < 0) {
            sendErrorResponse(correlationIdBytes);
        } else {
            sendSuccessResponse(correlationIdBytes, apiVersionValue);
        }
    }

    private void sendErrorResponse(byte[] correlationIdBytes) throws IOException {
        System.err.println("Sending error response to client: " + clientSocket.getRemoteSocketAddress());
        
        byte[] lengthBytes = ByteBuffer.allocate(4).putInt(7).array();
        byte[] errorCodeBytes = ByteBuffer.allocate(2).putShort((short) 35).array();
        byte[] emptyTaggedBuffer = new byte[]{0};
        
        clientSocket.getOutputStream().write(lengthBytes);
        clientSocket.getOutputStream().write(correlationIdBytes);
        clientSocket.getOutputStream().write(errorCodeBytes);
        clientSocket.getOutputStream().write(emptyTaggedBuffer);
        clientSocket.getOutputStream().flush();
    }

    private void sendSuccessResponse(byte[] correlationIdBytes, short apiVersionValue) throws IOException {
        System.err.println("Sending success response to client: " + clientSocket.getRemoteSocketAddress() + 
            " for API version: " + apiVersionValue);

        byte[] lengthBytes = ByteBuffer.allocate(4).putInt(19).array();
        byte[] errorCodeBytes = ByteBuffer.allocate(2).putShort((short) 0).array();
        byte[] numApiKeysBytes = new byte[]{2}; // Compact array format
        byte[] apiKeyBytes = ByteBuffer.allocate(2).putShort((short) 18).array();
        byte[] minVersionBytes = ByteBuffer.allocate(2).putShort((short) 0).array();
        byte[] maxVersionBytes = ByteBuffer.allocate(2).putShort((short) 4).array();
        byte[] taggedBuffer1 = new byte[]{0};
        byte[] throttleTimeBytes = ByteBuffer.allocate(4).putInt(0).array();
        byte[] taggedBuffer2 = new byte[]{0};

        // Send the complete response
        clientSocket.getOutputStream().write(lengthBytes);
        clientSocket.getOutputStream().write(correlationIdBytes);
        clientSocket.getOutputStream().write(errorCodeBytes);
        clientSocket.getOutputStream().write(numApiKeysBytes);
        clientSocket.getOutputStream().write(apiKeyBytes);
        clientSocket.getOutputStream().write(minVersionBytes);
        clientSocket.getOutputStream().write(maxVersionBytes);
        clientSocket.getOutputStream().write(taggedBuffer1);
        clientSocket.getOutputStream().write(throttleTimeBytes);
        clientSocket.getOutputStream().write(taggedBuffer2);
        clientSocket.getOutputStream().flush();

        System.err.println("Response sent successfully to client: " + clientSocket.getRemoteSocketAddress());
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
