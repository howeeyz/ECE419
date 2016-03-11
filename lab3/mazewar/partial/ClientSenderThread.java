import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.concurrent.BlockingQueue;

public class ClientSenderThread implements Runnable {

    private MSocket mSocket = null;
    private BlockingQueue<Event> eventQueue = null;
    private Player next = null;
    
    public ClientSenderThread(MSocket mSocket,
                              BlockingQueue eventQueue,
                              Player nextNode){
        this.mSocket = mSocket;
        this.eventQueue = eventQueue;
        this.next = nextNode;
    }
    
    public void run() {
        Event toServer = null;
        if(Debug.debug) System.out.println("Starting ClientSenderThread");
        while(true){
            try{                
                //Take packet from queue
                toServer = (Event)eventQueue.take();
                if(Debug.debug) System.out.println("Sending " + toServer);
                mSocket.writeObject(toServer);    
            }catch(InterruptedException e){
                e.printStackTrace();
                Thread.currentThread().interrupt();    
            }
            
        }
    }
}
