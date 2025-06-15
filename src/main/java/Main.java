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
      while(clientSocket.getInputStream().available() == 0) {
          Thread.sleep(1000);
      }

      BufferedInputStream in = new BufferedInputStream(clientSocket.getInputStream());
      byte[] messageSizeBytes = in.readNBytes(4);
      byte[] apiKey = in.readNBytes(2);
      byte[] apiVersion = in.readNBytes(2);
      int correlationId = ByteBuffer.wrap(in.readNBytes(4)).getInt();
      
      // Parse the API version (not API key!)
      short apiVersionValue = ByteBuffer.wrap(apiVersion).getShort();
      
      // Create response
      if (apiVersionValue > 4 || apiVersionValue < 0) {
        // Error response: length + correlation_id + error_code
        byte[] lengthBytes = ByteBuffer.allocate(4).putInt(6).array(); // 4 bytes correlation + 2 bytes error
        byte[] correlationBytes = ByteBuffer.allocate(4).putInt(correlationId).array();
        byte[] errorBytes = ByteBuffer.allocate(2).putShort((short) 35).array();
        
        clientSocket.getOutputStream().write(lengthBytes);
        clientSocket.getOutputStream().write(correlationBytes);
        clientSocket.getOutputStream().write(errorBytes);
      } else {
        // Success response: length + correlation_id + success_code
        byte[] lengthBytes = ByteBuffer.allocate(4).putInt(6).array(); // 4 bytes correlation + 2 bytes success
        byte[] correlationBytes = ByteBuffer.allocate(4).putInt(correlationId).array();
        byte[] successBytes = ByteBuffer.allocate(2).putShort((short) 0).array(); // 0 = no error
        
        clientSocket.getOutputStream().write(lengthBytes);
        clientSocket.getOutputStream().write(correlationBytes);
        clientSocket.getOutputStream().write(successBytes);
      }
      
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