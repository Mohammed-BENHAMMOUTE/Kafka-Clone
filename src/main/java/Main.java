
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class Main {

    public static void main(String[] args) {
        System.err.println("Logs from your program will appear here!");
        int port = 9092;

        try (ServerSocket serverSocket = new ServerSocket(port)) {
            serverSocket.setReuseAddress(true);
            System.err.println("Kafka server listening on port " + port);
            
            // Accept multiple clients concurrently
            while (true) {
                try {
                    // This blocks until a new client connects
                    Socket clientSocket = serverSocket.accept();
                    System.err.println("New client connected: " + clientSocket.getRemoteSocketAddress());
                    
                    // Create a new thread for each client connection
                    ClientHandler handler = new ClientHandler(clientSocket);
                    Thread clientThread = new Thread(handler);
                    clientThread.setName("Client-" + clientSocket.getRemoteSocketAddress());
                    clientThread.start();
                    
                } catch (IOException e) {
                    System.err.println("Error accepting client connection: " + e.getMessage());
                }
            }
        } catch (IOException e) {
            System.err.println("Server error: " + e.getMessage());
        }
    }
}
