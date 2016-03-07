import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class NamingService {
    
    //The maximum of clients that will join
    //Server waits until the max number of clients to join 
    private static final int MAX_CLIENTS = 2;
    private static ArrayList<Client> clientList;
    private MServerSocket mServerSocket = null;
    private int clientCount; //The number of clients before game starts
    private MSocket[] mSocketList = null; //A list of MSockets
    private BlockingQueue eventQueue = null; //A list of events
    
    
    
    /*
    * Constructor
    */
    public NamingService(int port) throws IOException{
        clientList = new ArrayList();   //Array list of registered clients
        clientCount = 0; 
        mServerSocket = new MServerSocket(port);
        if(Debug.debug) System.out.println("Listening on port: " + port);
        mSocketList = new MSocket[MAX_CLIENTS];
        eventQueue = new LinkedBlockingQueue<MPacket>();
    }
    
    //Get Player from naming service and send to client
    public Client getPlayer(String playerName){
       Iterator it = clientList.iterator();
       while(it.hasNext()){
           Client curr = (Client)it.next();
           if(curr.getName().equals(playerName))
               return curr;
       }
       return null;
    }
    
    //Add a new player to naming service
    public Client addPlayer(String player){
        if(getPlayer(player) != null)
            return null;
        
        Client newPlayer = new RemoteClient(player);
        clientList.add(newPlayer);
        clientCount++;
        return newPlayer;
    }
    
    /*
    *Starts the listener and sender threads 
    */
    public void startThreads() throws IOException{
        //Listen for new clients
        while(clientCount < MAX_CLIENTS){
            //Start a new listener thread for each new client connection
            MSocket mSocket = mServerSocket.accept();
            
            new Thread(new ServerListenerThread(mSocket, eventQueue)).start();
            
            mSocketList[clientCount] = mSocket;  
        }
        
        //Start a new sender thread 
        new Thread(new ServerSenderThread(mSocketList, eventQueue)).start();    
    }

        
    /*
    * Entry point for server
    */
    public static void main(String args[]) throws IOException {
        if(Debug.debug) System.out.println("Starting the server");
        int port = Integer.parseInt(args[0]);
        NamingService ns = new NamingService(port);
                
        ns.startThreads();    

    }
}
