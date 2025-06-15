import java.io.BufferedInputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;

public class Main {
  public static void main(String[] args) throws InterruptedException {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
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
        byte[] buffer = new byte[12];

        BufferedInputStream in = new BufferedInputStream(clientSocket.getInputStream());
        byte[] messageSizeBytes = in.readNBytes(4);
        byte[] apiKey = in.readNBytes(2);
        byte[] apiVersion = in.readNBytes(2);
        int correlationId = ByteBuffer.wrap(in.readNBytes(4)).getInt();
        

        clientSocket.getOutputStream().write(messageSizeBytes);
        var res = ByteBuffer.allocate(4).putInt(correlationId).array();
        clientSocket.getOutputStream().write(res);
        
        int int_api_version = ByteBuffer.wrap(apiKey).getInt();

        if(int_api_version < 0 || int_api_version >4) {
          var error_response = ByteBuffer.allocate(2).putInt(35).array();
          clientSocket.getOutputStream().write(error_response);
        };
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
