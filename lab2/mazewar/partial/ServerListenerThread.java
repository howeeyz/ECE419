import java.io.IOException;
import java.util.concurrent.BlockingQueue;

public class ServerListenerThread implements Runnable {

    private MSocket mSocket =  null;
    private BlockingQueue eventQueue = null;

    public ServerListenerThread( MSocket mSocket, BlockingQueue eventQueue){
        this.mSocket = mSocket;
        this.eventQueue = eventQueue;
    }

    public void run() {
        MPacket received = null;
        if(Debug.debug) System.out.println("Starting a listener");
        while(true){
            try{
                received = (MPacket) mSocket.readObject();
                System.out.println("Received: " + received);
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
