import java.io.IOException;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;

public class ServerListenerThread implements Runnable {

    private MSocket mSocket =  null;
    private BlockingQueue clientQueue = null;
    private Map clientPriorityMap = null;
    private Map expectedSequenceMap = null;

    public ServerListenerThread( MSocket mSocket, BlockingQueue clientQueue, Map clientPriorityMap, Map expectedSequenceMap){
        this.mSocket = mSocket;
        this.clientQueue = clientQueue;
        this.clientPriorityMap = clientPriorityMap;
        this.expectedSequenceMap = expectedSequenceMap;
    }

    public void run() {
        MPacket received = null;
        if(Debug.debug) System.out.println("Starting a listener");
        while(true){
            try{
                if(Debug.debug) System.out.println("Received: " + received);
                received = (MPacket) mSocket.readObject();
                clientQueue.put(received.name);
                if(clientPriorityMap.isEmpty() || clientPriorityMap.containsKey(received.name) == false){
                    PriorityBlockingQueue pQueue = new PriorityBlockingQueue<MPacket>(20, new EventComparator());
                    pQueue.put(received);
                    clientPriorityMap.put(received.name, pQueue);
                }
                else{
                    PriorityBlockingQueue pQueue = (PriorityBlockingQueue) clientPriorityMap.get(received.name);
                    pQueue.put(received);
                    clientPriorityMap.put(received.name, pQueue);
                }
                if(expectedSequenceMap.isEmpty() || expectedSequenceMap.containsKey(received.name) == false){
                    expectedSequenceMap.put(received.name, 0);
                }
                
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
