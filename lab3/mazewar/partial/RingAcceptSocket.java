
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author zhangh55
 */
public class RingAcceptSocket {
    /* This is the serverSocket equivalent to 
    * RingSocket
    */
 
    private ServerSocket serverSocket = null;
    
    /*
     *This creates a server socket
     */    
    public RingAcceptSocket(int port) throws IOException{
        serverSocket = new ServerSocket(port);
    }
    
    public RingSocket accept() throws IOException{
        Socket socket = serverSocket.accept(); 
        RingSocket rSocket = new RingSocket(socket);
        return rSocket;
    }
}
