import java.io.InvalidObjectException;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.Random;

public class NamingServiceSenderThread implements Runnable {

    //private ObjectOutputStream[] outputStreamList = null;
    private MSocket[] mSocketList = null;
    private BlockingQueue eventQueue = null;
    private int globalSequenceNumber; 
    
    public NamingService mNamingService;
    
    public NamingServiceSenderThread(MSocket[] mSocketList,
                              BlockingQueue eventQueue){
        this.mSocketList = mSocketList;
        this.eventQueue = eventQueue;
        this.globalSequenceNumber = 0;
    }

    /*
     *Handle the initial joining of players including 
      position initialization
     */
    public void handleHello(){
        
        //The number of players
        int playerCount = mSocketList.length;
        Player[] players = new Player[playerCount];
        if(Debug.debug) System.out.println("In handleHello");
        NSPacket hello = null;
        try{        
            for(int i=0; i<playerCount; i++){
                hello = (NSPacket)eventQueue.take();
                //Sanity check 
                if(hello.getmAckNo() > -1){
                    throw new InvalidObjectException("Expecting a sequence packet. Not an acknowledgement");
                }
            }
            assert(hello.getmPlayerName() != null);
            mNamingService.addPlayer(hello.getmPlayerName(), hello.mazeSeed, hello.mazeWidth, hello.mazeHeight);
            
            NSPacket toBroadcast = new NSPacket(NamingService.NAMING_SERVICE_STRING, NamingService.BROADCAST_STRING, mNamingService.getPlayerList());
            toBroadcast.setSeqNo(globalSequenceNumber++);
            
            //Now broadcast the new packet
            if(Debug.debug) System.out.println("Sending " + hello);
            for(MSocket mSocket: mSocketList){
                mSocket.writeObject(toBroadcast);   
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
        
//        while(true){
//            try{
//                //Take packet from queue to broadcast
//                //to all clients
//                toBroadcast = (MPacket)eventQueue.take();
//                //Tag packet with sequence number and increment sequence number
//                toBroadcast.sequenceNumber = this.globalSequenceNumber++;
//                if(Debug.debug) System.out.println("Sending " + toBroadcast);
//                //Send it to all clients
//                for(MSocket mSocket: mSocketList){
//                    mSocket.writeObject(toBroadcast);
//                }
//            }catch(InterruptedException e){
//                System.out.println("Throwing Interrupt");
//                Thread.currentThread().interrupt();    
//            }
//            
//        }
    }
}
