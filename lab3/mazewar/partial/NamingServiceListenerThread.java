import java.io.IOException;
import java.util.concurrent.BlockingQueue;

public class NamingServiceListenerThread implements Runnable{
    
    private MSocket mSocket =  null;
    private BlockingQueue eventQueue = null;

    public NamingServiceListenerThread(MSocket mSocket, BlockingQueue eventQueue){
        this.mSocket = mSocket;
        this.eventQueue = eventQueue;
    }

    public void run() {
        NSPacket received = null;
        if(Debug.debug) System.out.println("Starting a listener");
        while(true){
            try{
                received = (NSPacket) mSocket.readObject();
                eventQueue.put(received);    
            }catch(InterruptedException e){
                Thread.currentThread().interrupt();    
            }catch(IOException e){
                Thread.currentThread().interrupt();
            }catch(ClassNotFoundException e){
                Thread.currentThread().interrupt();    
            }
            
        }
    }
    
}
