import java.util.concurrent.BlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.Hashtable;
/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author zhangh55
 */
public class ClientReceiverThread implements Runnable {
    private Hashtable<String, Client> mClientTable = null;
    private PriorityBlockingQueue mEventQueue = null;
    private Client mClient = null;
    private Integer mExpectedSequenceNum;
    
    public ClientReceiverThread(Hashtable<String, Client> clientTable,
                                PriorityBlockingQueue eventQueue){
        
        this.mClientTable = clientTable;
        this.mEventQueue = eventQueue;
        this.mExpectedSequenceNum = 0;
        
        if(Debug.debug) System.out.println("Instatiating ClientReceiverThread");
    }
    
    public void run(){
        while(true){
            try{
                MPacket received = (MPacket)mEventQueue.peek();
                if(received == null || received.sequenceNumber != this.mExpectedSequenceNum){
                    continue;
                }
                this.mExpectedSequenceNum++;
                mEventQueue.take();
                System.out.println(received.sequenceNumber);
                mClient = mClientTable.get(received.name);
                if(received.event == MPacket.UP){
                    mClient.forward();
                }else if(received.event == MPacket.DOWN){
                    mClient.backup();
                }else if(received.event == MPacket.LEFT){
                    mClient.turnLeft();
                }else if(received.event == MPacket.RIGHT){
                    mClient.turnRight();
                }else if(received.event == MPacket.FIRE){
                    mClient.fire();
                }else{
                    throw new UnsupportedOperationException();
                }  
            }
            catch(InterruptedException e){
                e.printStackTrace();
            }


        }
    }
    
}
