import java.util.concurrent.BlockingQueue;
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
    private BlockingQueue mEventQueue = null;
    private Client mClient = null;
    
    public ClientReceiverThread(Hashtable<String, Client> clientTable,
                                BlockingQueue eventQueue){
        
        this.mClientTable = clientTable;
        this.mEventQueue = eventQueue;
        
        if(Debug.debug) System.out.println("Instatiating ClientReceiverThread");
    }
    
    public void run(){
        while(true){
            try{
                MPacket received = (MPacket)mEventQueue.take();
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
