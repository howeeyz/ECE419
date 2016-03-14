import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.BlockingQueue;

public class ClientSenderThread implements Runnable {
    private final int TIMEOUT = 500;
    private RingSocket ringSocket = null;
    private BlockingQueue<Token> mTQueue = null;
    private Player myPlayer = null;
    
    private NSPacket received = null;
    private boolean acked = false;
    
    private Timer ackTimer = null;
    
    public ClientSenderThread(RingSocket rSocket,
                              BlockingQueue tQueue,
                              Player player){
        this.ringSocket = rSocket;
        this.mTQueue = tQueue;
        this.myPlayer = player;
    }
    
    private void scheduleTimer(final Token tk){

        TimerTask taskPerformer = new TimerTask() {
            public void run() {
                retransmitToken(tk);
            }
        };
        
        if(ackTimer == null){
            ackTimer = new Timer("AckTimer", true);
        }
        ackTimer.scheduleAtFixedRate(taskPerformer, TIMEOUT, TIMEOUT);
    }
    
    private void retransmitToken(final Token tk){
        ringSocket.writeObject(tk);
    }
    
    public void run() {
        if(Debug.debug) System.out.println("Starting ClientSenderThread");
        while(true){      
            if(!mTQueue.isEmpty()){
                
                try{
                    //This tells us that the listener is done doing what it needs to do
                    //Ready to pass off token

                    Token tk = mTQueue.peek();
                    scheduleTimer(tk);
                    ringSocket.writeObject(tk);
                    
                    mTQueue.take();
                    
                    received = (NSPacket)ringSocket.readObject();
                    
                    while(received == null){
                        if(!mTQueue.isEmpty() && mTQueue.peek().getCount() > tk.getCount()){
                            break;
                        }
                        received = (NSPacket)ringSocket.readObject();
                    }
                    
                    // If we receive a newer token from the previous node
                    // Or if we receive an ack from the next node
                    // Stop retransmitting packets
                    if((!mTQueue.isEmpty() && mTQueue.peek().getCount() > tk.getCount()) 
                        || tk.getCount() == received.getmAckNo()){
                        assert(ackTimer != null);
                        ackTimer.cancel();
                        ackTimer.purge();
                        ackTimer = null;
                    }
                    
                }catch (InterruptedException e){
                    System.out.println(e);
                }catch (IOException e){
                    System.out.println(e);
                }catch (ClassNotFoundException e){
                    System.out.println(e);
                }
            }
        }
    }
}
