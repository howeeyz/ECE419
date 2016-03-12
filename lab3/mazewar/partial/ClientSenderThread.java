import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.concurrent.BlockingQueue;

public class ClientSenderThread implements Runnable {

    private RingSocket ringSocket = null;
    private BlockingQueue<Token> mTQueue = null;
    private TokenWrapper mTkWrapper = null;
    
    public ClientSenderThread(RingSocket rSocket,
                              BlockingQueue tQueue){
        this.ringSocket = rSocket;
        this.mTQueue = tQueue;
    }
    
    public void run() {
        if(Debug.debug) System.out.println("Starting ClientSenderThread");
        while(true){      
            if(!mTQueue.isEmpty()){
                
                System.out.println("Sending now!");
                //This tells us that the listener is done doing what it needs to do
                //Ready to pass off token

                ringSocket.writeObject(mTQueue.peek());

                try{
                    mTQueue.take();
                }catch (InterruptedException e){
                    System.out.println(e);
                }
                //TODO: Deal with ACK here
                

//                    boolean acked = false;
//                    while(!acked ){
//                        AckPacket resp = (AckPacket)ringSocket.readObject();
//                        acked = resp.acked;
//                    }
            }
        }
    }
}
