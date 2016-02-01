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
    private MazeImpl mMaze = null;
    
    public ClientReceiverThread(Hashtable<String, Client> clientTable,
                                PriorityBlockingQueue eventQueue,
                                MazeImpl maze){
        
        this.mClientTable = clientTable;
        this.mEventQueue = eventQueue;
        this.mExpectedSequenceNum = 0;
        this.mMaze = maze;
        
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
                }else if(received.event == MPacket.MISS){
                    Projectile prj = mMaze.getProjectileForClientName(received.prj1Owner);
                    assert(prj != null);
                    mMaze.missHandler(prj);
                }else if(received.event == MPacket.HIT){
                    Client source = mClientTable.get(received.source);
                    Client target = mClientTable.get(received.target);
                    
                    Projectile prj = mMaze.getProjectileForClientName(received.prj1Owner);
                    assert(prj != null);
                    mMaze.hitHandler(prj, source, target);
                }else if(received.event == MPacket.COLLISION){
                    Projectile prj1 = mMaze.getProjectileForClientName(received.prj1Owner);
                    Projectile prj2 = mMaze.getProjectileForClientName(received.prj2Owner);
                    assert(prj1 != null &&  prj2 != null);
                    mMaze.collisionHandler(prj1, prj2);
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
