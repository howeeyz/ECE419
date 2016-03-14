import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.BlockingQueue;

public class ClientSenderThread implements Runnable {
    private final int TIMEOUT = 100;
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
    
    private void transmitToken(final Token tk){

        TimerTask taskPerformer = new TimerTask() {
            public void run() {
                transmitToken(tk);
            }
        };
        
        ackTimer = new Timer("AckTimer", true);
        ackTimer.schedule(taskPerformer, TIMEOUT);
        ringSocket.writeObject(tk);
    }
    
    public void run() {
        if(Debug.debug) System.out.println("Starting ClientSenderThread");
        while(true){      
            if(!mTQueue.isEmpty()){
                
                try{
                    System.out.println("Sending Token!");
                    //This tells us that the listener is done doing what it needs to do
                    //Ready to pass off token

                    Token tk = mTQueue.peek();
                    transmitToken(tk);
                    
                    received = (NSPacket)ringSocket.readObject();
                    
                    while (received == null){
                        ;
                    }
                    
                    if(tk.getCount() == received.getmAckNo()){
                        System.out.println("Ack received. Kill timeout, pop token");
                        assert(ackTimer != null);
                        ackTimer.cancel();
                        ackTimer.purge();
                        mTQueue.take();
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
