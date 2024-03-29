import java.net.Socket;
import java.net.ServerSocket;
import java.io.IOException;

public class MNamingSocket{
    /*
    * This is the serverSocket equivalent to 
    * MSocket
    */
 
    private ServerSocket serverSocket = null;
    
    /*
     *This creates a server socket
     */    
    public MNamingSocket(int port) throws IOException{
        serverSocket = new ServerSocket(port);
    }
    
    public MSocket accept() throws IOException{
        Socket socket = serverSocket.accept(); 
        MSocket mSocket = new MSocket(socket);
        return mSocket;
    }
}