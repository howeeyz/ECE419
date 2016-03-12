import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class NamingService {
    
    //The maximum of clients that will join
    //Server waits until the max number of clients to join 
    public static final String BROADCAST_STRING = "Broadcast";
    public static final String NAMING_SERVICE_STRING = "Naming Service";
    private static final int MAX_CLIENTS = 2;
    private static ArrayList<Player> playerList;
    private NamingServiceSocket mNamingServiceSocket = null;
    private int clientCount; //The number of clients before game starts
    private MSocket[] mSocketList = null; //A list of MSockets
    private BlockingQueue eventQueue = null; //A list of events
    
    
    
    /*
    * Constructor
    */
    public NamingService(int port) throws IOException{
        playerList = new ArrayList();   //Array list of registered clients
        clientCount = 0; 
        mNamingServiceSocket = new NamingServiceSocket(port);
        if(Debug.debug) System.out.println("Listening on port: " + port);
        mSocketList = new MSocket[MAX_CLIENTS];
        eventQueue = new LinkedBlockingQueue<NSPacket>();
    }
    
    //Get Player from naming service and send to client
    public Player getPlayer(String playerName){
       Iterator it = playerList.iterator();
       while(it.hasNext()){
           Player curr = (Player)it.next();
           if(curr.name.equals(playerName))
               return curr;
       }
       return null;
    }
    
    //Add a new player to naming service
    public Player addPlayer(String playerName, int mazeSeed, int mazeWidth, int mazeHeight, String host, int port){
        if(getPlayer(playerName) != null)
            return null;
        
        Random randomGen = null;
        
        if(randomGen == null){
            randomGen = new Random(mazeSeed); 
        }
        //Get a random location for player
        Point point = new Point(randomGen.nextInt(mazeWidth),
                          randomGen.nextInt(mazeHeight));
        
        Player newPlayer = new Player(playerName, point, Player.North, host, port);
        playerList.add(newPlayer);
        return newPlayer;
    }
    
    public ArrayList getPlayerList(){
        return playerList;
    }
    
    /*
    *Starts the listener and sender threads 
    */
    public void startThreads() throws IOException{
        //Listen for new clients
        while(clientCount < MAX_CLIENTS){
            //Start a new listener thread for each new client connection
            MSocket mSocket = mNamingServiceSocket.accept();
            
            new Thread(new NamingServiceListenerThread(mSocket, eventQueue)).start();
            
            mSocketList[clientCount] = mSocket;  
            
            clientCount++;
        }
        
        System.out.println("Starting NamingServiceSenderThread");
        //Start a new sender thread 
        NamingServiceSenderThread nsst = new NamingServiceSenderThread(mSocketList, eventQueue);
        nsst.mNamingService = this;
        new Thread(nsst).start();    
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
