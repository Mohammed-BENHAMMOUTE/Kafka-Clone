import java.io.BufferedInputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;

public class Main {

    public static void main(String[] args) throws InterruptedException {
        System.err.println("Logs from your program will appear here!");
        ServerSocket serverSocket = null;
        Socket clientSocket = null;
        int port = 9092;
        try {
            serverSocket = new ServerSocket(port);
            serverSocket.setReuseAddress(true);
            clientSocket = serverSocket.accept();
            while (clientSocket.getInputStream().available() == 0) {
                Thread.sleep(1000);
            }

            BufferedInputStream in = new BufferedInputStream(clientSocket.getInputStream());
            byte[] messageSizeBytes = in.readNBytes(4);
            byte[] apiKey = in.readNBytes(2);
            byte[] apiVersion = in.readNBytes(2);
            int correlationId = ByteBuffer.wrap(in.readNBytes(4)).getInt();

            // Parse the API version (not API key!)
            short apiVersionValue = ByteBuffer.wrap(apiVersion).getShort();
            byte[] correlationBytes = ByteBuffer.allocate(4).putInt(correlationId).array();
            
            System.err.println("Received API version: " + apiVersionValue);
            System.err.println("Correlation ID: " + correlationId);
            
            // Create APIVersions response
            if (apiVersionValue > 4 || apiVersionValue < 0) {
                // Error response: just correlation_id + error_code + empty tagged buffer
                byte[] lengthBytes = ByteBuffer.allocate(4).putInt(7).array(); // 4 bytes correlation + 2 bytes error + 1 byte tagged buffer
                byte[] errorCodeBytes = ByteBuffer.allocate(2).putShort((short) 35).array();
                byte[] emptyTaggedBuffer = new byte[]{0}; // 0 tagged fields
                
                clientSocket.getOutputStream().write(lengthBytes);
                clientSocket.getOutputStream().write(correlationBytes);
                clientSocket.getOutputStream().write(errorCodeBytes);
                clientSocket.getOutputStream().write(emptyTaggedBuffer);
            } else {
                // Success response: full APIVersions format
                // Calculate total response length:
                // 4 bytes correlation_id + 2 bytes error_code + 1 byte num_api_keys + 
                // 6 bytes api_key_data (2+2+2) + 1 byte TAG_BUFFER + 4 bytes throttle_time + 1 byte TAG_BUFFER = 21 bytes
                byte[] lengthBytes = ByteBuffer.allocate(4).putInt(21).array();
                
                // Error code (0 = success)
                byte[] errorCodeBytes = ByteBuffer.allocate(2).putShort((short) 0).array();
                
                // Number of API keys (1 for APIVersions only)
                byte[] numApiKeysBytes = new byte[]{1}; // INT8 = 1 byte
                
                // API key entry: api_key=18, min_version=0, max_version=4
                byte[] apiKeyBytes = ByteBuffer.allocate(2).putShort((short) 18).array(); // api_key = 18 (APIVersions)
                byte[] minVersionBytes = ByteBuffer.allocate(2).putShort((short) 0).array(); // min_version = 0
                byte[] maxVersionBytes = ByteBuffer.allocate(2).putShort((short) 4).array(); // max_version = 4
                
                // Tagged buffer (empty)
                byte[] taggedBuffer1 = new byte[]{0}; // 0 tagged fields
                
                // Throttle time (0 ms)
                byte[] throttleTimeBytes = ByteBuffer.allocate(4).putInt(0).array();
                
                // Final tagged buffer (empty)
                byte[] taggedBuffer2 = new byte[]{0}; // 0 tagged fields
                
                // Send the complete response
                clientSocket.getOutputStream().write(lengthBytes);
                clientSocket.getOutputStream().write(correlationBytes);
                clientSocket.getOutputStream().write(errorCodeBytes);
                clientSocket.getOutputStream().write(numApiKeysBytes);
                clientSocket.getOutputStream().write(apiKeyBytes);
                clientSocket.getOutputStream().write(minVersionBytes);
                clientSocket.getOutputStream().write(maxVersionBytes);
                clientSocket.getOutputStream().write(taggedBuffer1);
                clientSocket.getOutputStream().write(throttleTimeBytes);
                clientSocket.getOutputStream().write(taggedBuffer2);
            }
            
            // Flush the output stream to ensure all data is sent
            clientSocket.getOutputStream().flush();

        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
        } finally {
            try {
                if (clientSocket != null) {
                    clientSocket.close();
                }
            } catch (IOException e) {
                System.out.println("IOException: " + e.getMessage());
            }
        }
    }
}