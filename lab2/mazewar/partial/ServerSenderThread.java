import java.io.InvalidObjectException;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.Random;

public class ServerSenderThread implements Runnable {

    //private ObjectOutputStream[] outputStreamList = null;
    private MSocket[] mSocketList = null;
    private BlockingQueue clientQueue = null;
    private Map clientPriorityMap = null;
    private Map expectedSequenceMap = null;
    private int globalSequenceNumber; 
    
    private String currentClient = null;
    
    public ServerSenderThread(MSocket[] mSocketList,
                              BlockingQueue clientQueue,
                              Map clientPriorityMap,
                              Map expectedSequenceMap){
        this.mSocketList = mSocketList;
        this.clientQueue = clientQueue;
        this.clientPriorityMap = clientPriorityMap;
        this.expectedSequenceMap = expectedSequenceMap;
        this.globalSequenceNumber = 0;
    }

    /*
     *Handle the initial joining of players including 
      position initialization
     */
    public void handleHello(){
        
        //The number of players
        int playerCount = mSocketList.length;
        Random randomGen = null;
        Player[] players = new Player[playerCount];
        if(Debug.debug) System.out.println("In handleHello");
        MPacket hello = null;
        try{        
            for(int i=0; i<playerCount; i++){
                while(clientQueue.isEmpty()){
                    
                }
                PriorityBlockingQueue pQueue =  (PriorityBlockingQueue)clientPriorityMap.get(clientQueue.take());  
                hello = (MPacket) pQueue.take();
                //Sanity check 
                if(hello.type != MPacket.HELLO){
                    throw new InvalidObjectException("Expecting HELLO Packet");
                }
                if(randomGen == null){
                   randomGen = new Random(hello.mazeSeed); 
                }
                //Get a random location for player
                Point point =
                    new Point(randomGen.nextInt(hello.mazeWidth),
                          randomGen.nextInt(hello.mazeHeight));
                
                //Start them all facing North
                Player player = new Player(hello.name, point, Player.North);
                players[i] = player;
            }
            
            hello.event = MPacket.HELLO_RESP;
            hello.players = players;
            //Now broadcast the HELLO
            if(Debug.debug) System.out.println("Sending " + hello);
            for(MSocket mSocket: mSocketList){
                mSocket.writeObject(hello);   
            }
        }catch(InterruptedException e){
            e.printStackTrace();
            Thread.currentThread().interrupt();    
        }catch(IOException e){
            e.printStackTrace();
            Thread.currentThread().interrupt();
        }
    }
    
    public void run() {
        MPacket toBroadcast = null;
        
        handleHello();
        
        while(true){
            try{
                //Take next client from clientQueue
                if(currentClient == null){
                    if(clientQueue.isEmpty()){
                        continue;
                    }
                    currentClient = (String) clientQueue.take();
                }
                
                Integer expected = (Integer)(expectedSequenceMap.get(currentClient));
                
                PriorityBlockingQueue pQueue = (PriorityBlockingQueue)clientPriorityMap.get(currentClient);
                if(pQueue.isEmpty()){
                    currentClient = null;
                    continue;
                }
                MPacket packet = (MPacket)pQueue.peek();
                
                if(expected != packet.sequenceNumber){
                    continue;
                }
                //Expected sequence number is equal.
                
                while(!pQueue.isEmpty()){
                    MPacket p = (MPacket)pQueue.take();
                    if(p.sequenceNumber != expected){
                        break;
                    }
                    toBroadcast = p;
                    //Tag packet with sequence number and increment sequence number
                    toBroadcast.sequenceNumber = this.globalSequenceNumber++;
                    if(Debug.debug) System.out.println("Sending " + toBroadcast);
                    //Send it to all clients
                    for(MSocket mSocket: mSocketList){
                        mSocket.writeObject(toBroadcast);
                    }
                    
                    expectedSequenceMap.put(currentClient, ++expected);
                }
                if(pQueue.isEmpty()){
                    currentClient = null;
                }
            }catch(InterruptedException e){
                System.out.println("Throwing Interrupt");
                Thread.currentThread().interrupt();    
            }
            
        }
    }
}
