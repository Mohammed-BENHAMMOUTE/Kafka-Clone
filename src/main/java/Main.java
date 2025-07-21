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

            // Parse the API key and version
            short apiKeyValue = ByteBuffer.wrap(apiKey).getShort();
            short apiVersionValue = ByteBuffer.wrap(apiVersion).getShort();
            byte[] correlationBytes = ByteBuffer.allocate(4).putInt(correlationId).array();
            
            System.err.println("Received API key: " + apiKeyValue);
            System.err.println("Received API version: " + apiVersionValue);
            System.err.println("Correlation ID: " + correlationId);
            
            // Check if this is an APIVersions request (API key 18)
            if (apiKeyValue != 18) {
                System.err.println("Error: Expected API key 18 (APIVersions), got: " + apiKeyValue);
                return;
            }
            
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
                System.err.println("Sending success response for API version: " + apiVersionValue);
                
                // Total response length: 4 bytes correlation_id + 17 bytes response body = 21 bytes
                byte[] lengthBytes = ByteBuffer.allocate(4).putInt(21).array();
                
                // Error code (0 = success)
                byte[] errorCodeBytes = ByteBuffer.allocate(2).putShort((short) 0).array();
                
                // Number of API keys (2 = 1 element, because 0=null, 1=empty, 2=1 element)
                byte[] numApiKeysBytes = new byte[]{2}; // INT8 = 1 byte, value = 2 for 1 API key
                
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
                
                System.err.println("Total response length: 21 bytes");
                
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
                
                System.err.println("Response sent successfully");
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