/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

import java.util.ArrayList;
import java.lang.String;
import java.util.Iterator;
import java.io.IOException;

public class NamingService {
    
    private static int max_clients = 0;
    private static ArrayList<Client> clientList;
    private static int clientCount;
    private MNamingSocket mServerSocket = null;
    private MSocket[] mSocketList = null; //A list of MSockets
    
    public NamingService(int port) throws IOException{
        clientList = new ArrayList();
        clientCount = 0;
//        Client newPlayer = new GUIClient(player);
    }
    
    public Client getPlayer(String playerName){
       Iterator it = clientList.iterator();
       while(it.hasNext()){
           Client curr = (Client)it.next();
           if(curr.getName().equals(playerName))
               return curr;
       }
       return null;
    }
    
    public Client addPlayer(String player){
        
        if(getPlayer(player) != null)
            return null;
        
        Client newPlayer = new RemoteClient(player);
        clientList.add(newPlayer);
        clientCount++;
        return newPlayer;
    }
    
    public void startThreads() throws IOException {
        //Listen for new clients
        while(clientCount < max_clients){
            //Start a new listener thread for each new client connection
            MSocket mSocket = mServerSocket.accept();
            
            new Thread(new NamingServiceListenerThread(mSocket, eventQueue)).start();
            
            mSocketList[clientCount] = mSocket;
            
            
            clientCount++;
        }
        
        //Start a new sender thread 
        new Thread(new ServerSenderThread(mSocketList, eventQueue)).start(); 
    }
    
    public static void main(String args[]) throws IOException {
        if(Debug.debug) System.out.println("Starting the Naming Service");
        int port = Integer.parseInt(args[0]);
        max_clients = Integer.parseInt(args[1]);
        NamingService namingService  = new NamingService(port);
                
        namingService.startThreads();
    }
    
}


///*
//private static int max_clients = 0;
//    private MServerSocket mServerSocket = null;
//    private int clientCount; //The number of clients before game starts
//    private MSocket[] mSocketList = null; //A list of MSockets
//    private BlockingQueue eventQueue = null; //A list of events
//    
//    /*
//    * Constructor
//    */
//    public Server(int port) throws IOException{
//        clientCount = 0; 
//        mServerSocket = new MServerSocket(port);
//        if(Debug.debug) System.out.println("Listening on port: " + port);
//        mSocketList = new MSocket[max_clients];
//        eventQueue = new LinkedBlockingQueue<MPacket>();
//    }
//    
//    /*
//    *Starts the listener and sender threads 
//    */
//    public void startThreads() throws IOException{
//        //Listen for new clients
//        while(clientCount < max_clients){
//            //Start a new listener thread for each new client connection
//            MSocket mSocket = mServerSocket.accept();
//            
//            new Thread(new ServerListenerThread(mSocket, eventQueue)).start();
//            
//            mSocketList[clientCount] = mSocket;                            
//            
//            clientCount++;
//        }
//        
//        //Start a new sender thread 
//        new Thread(new ServerSenderThread(mSocketList, eventQueue)).start();    
//    }
//
//        
//    /*
//    * Entry point for server
//    */
//    public static void main(String args[]) throws IOException {
//        if(Debug.debug) System.out.println("Starting the server");
//        int port = Integer.parseInt(args[0]);
//        max_clients = Integer.parseInt(args[1]);
//        Server server = new Server(port);
//                
//        server.startThreads();    
//
//    }
//
//
//*/