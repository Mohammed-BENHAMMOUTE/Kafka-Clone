import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

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
       };
         // Wait for client to send data
           // I need to extract the correlation Id
       int correlationId = 0;
       byte[] buffer = new byte[12];
       int bytesRead = clientSocket.getInputStream().read(buffer);
      if (bytesRead == -1) {
         System.out.println("No data received from client.");
         return;
      }else{
//                 Extract the correlation ID from the buffer
        correlationId = ((buffer[8] & 0xFF) << 24) | ((buffer[9] & 0xFF) << 16) |
                        ((buffer[10] & 0xFF) << 8) | (buffer[11] & 0xFF);

        System.out.println("Correlation ID: " + correlationId);
        // send the response back to the client with the correlation ID
          byte response[] = new byte[8];
        response[0] = 0; response[1] = 0; response[2] = 0; response[3] = 0;
        response[4] = (byte) ((correlationId >> 24) &0xFF);
        response[5] = (byte) ((correlationId >> 16) &0xFF);
        response[6] = (byte) ((correlationId >> 8) &0xFF);
        response[7] = (byte) (correlationId & 0xFF);
        clientSocket.getOutputStream().write(response);
//        clientSocket.getOutputStream().write(new byte[]{0,0,0,0,});

//              for (int i = 0; i < bytesRead; i++) {
//                System.out.printf("Byte %d: %02X%n", i, buffer[i]);
//              }


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
