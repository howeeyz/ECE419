import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;

public class Server {
    
	//The maximum of clients that will join
	//Server waits until the max number of clients to join 
    private static final int MAX_CLIENTS = 2;
    private MServerSocket mServerSocket = null;
    private int clientCount; //The number of clients before game starts
    private MSocket[] mSocketList = null; //A list of MSockets
    private BlockingQueue clientQueue = null; //A list of events
    private Map clientPriorityMap = null;
    private Map expectedSequenceMap = null;
    
    /*
    * Constructor
    */
    public Server(int port) throws IOException{
        clientCount = 0; 
        mServerSocket = new MServerSocket(port);
        if(Debug.debug) System.out.println("Listening on port: " + port);
        mSocketList = new MSocket[MAX_CLIENTS];
        clientQueue = new LinkedBlockingQueue<MPacket>();
        clientPriorityMap = new HashMap();
        expectedSequenceMap = new HashMap();
    }
    
    /*
    *Starts the listener and sender threads 
    */
    public void startThreads() throws IOException{
        //Listen for new clients
        while(clientCount < MAX_CLIENTS){
            //Start a new listener thread for each new client connection
            MSocket mSocket = mServerSocket.accept();
            
            new Thread(new ServerListenerThread(mSocket, clientQueue, clientPriorityMap, expectedSequenceMap)).start();
            
            mSocketList[clientCount] = mSocket;                            
            
            clientCount++;
        }
        
        //Start a new sender thread 
        new Thread(new ServerSenderThread(mSocketList, clientQueue, clientPriorityMap, expectedSequenceMap)).start();    
    }

        
    /*
    * Entry point for server
    */
    public static void main(String args[]) throws IOException {
        if(Debug.debug) System.out.println("Starting the server");
        int port = Integer.parseInt(args[0]);
        Server server = new Server(port);
                
        server.startThreads();    

    }
}
