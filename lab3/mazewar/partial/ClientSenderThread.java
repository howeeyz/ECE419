import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.concurrent.BlockingQueue;

public class ClientSenderThread implements Runnable {

    private RingSocket ringSocket = null;
    //private BlockingQueue<Event> eventQueue = null;
    private BlockingQueue<Token> eventQueue = null;
    private Player next = null;
    
    public ClientSenderThread(RingSocket rSocket,
                              BlockingQueue eventQueue,
                              Player nextNode){
        this.ringSocket = rSocket;
        this.eventQueue = eventQueue;
        this.next = nextNode;
    }
    
    public void run() {
        if(Debug.debug) System.out.println("Starting ClientSenderThread");
        while(true){            
            if(Mazewar.token != null){
                //This tells us that the listener is done doing what it needs to do
                //Ready to pass off token
                ringSocket.writeObject(Mazewar.token);
                
                //TODO: Deal with ACK here
                
                Mazewar.token = null;

//                    boolean acked = false;
//                    while(!acked ){
//                        AckPacket resp = (AckPacket)ringSocket.readObject();
//                        acked = resp.acked;
//                    }

            }
        }
    }
}
