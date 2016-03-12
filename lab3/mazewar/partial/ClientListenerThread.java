import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Hashtable;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ClientListenerThread implements Runnable {

    private RingSocket ringSocket  =  null;
    private Hashtable<String, Client> clientTable = null;
    private Player myPlayer;
    private BlockingQueue<Event> mEventQueue = null;
    private BlockingQueue<Token> mTQueue = null;

    public ClientListenerThread(RingSocket rSocket,
                                Hashtable<String, Client> clientTable,
                                BlockingQueue<Event> eventQueue,
                                BlockingQueue<Token> tQueue,
                                Player player){
        this.ringSocket = rSocket;
        this.clientTable = clientTable;
        this.mEventQueue = eventQueue;
        this.mTQueue = tQueue;
        this.myPlayer = player;
        if(Debug.debug) System.out.println("Instatiating ClientListenerThread");
    }

    public void run() {
        Token received = null;
        Client client = null;
        int lastSeenTokenCount = -1;
        if(Debug.debug) System.out.println("Starting ClientListenerThread");
        while(true){
            try{
                received = (Token) ringSocket.readObject();
                
                if(null == received || lastSeenTokenCount == received.getCount()){
                    continue;
                }
                //Send an ACK back to the previous node, indicating we successfully
                //retrieved the token
                ringSocket.writeObject(new NSPacket(received.getCount()));
                System.out.println(received.getCount());

                Queue<Event> gQueue = received.getGlobalQueue();
                
                Event seenEvent = null;
                Event currEvent = null;
                
                boolean seenSet = false;
                
                while(!gQueue.isEmpty()){
                    if(gQueue.peek().name.equals(myPlayer.name)){
                        gQueue.remove();
                        continue;
                    }
                    
                    if(seenEvent == gQueue.peek()){
                        break;
                    }
                    
                    currEvent = gQueue.remove();
                    
                    client = clientTable.get(currEvent.name);
                    if(currEvent.event == Event.UP){
                        client.forward();
                    }else if(currEvent.event == Event.DOWN){
                        client.backup();
                    }else if(currEvent.event == Event.LEFT){
                        client.turnLeft();
                    }else if(currEvent.event == Event.RIGHT){
                        client.turnRight();
                    }else if(currEvent.event == Event.FIRE){
                        client.fire();
                    }else if(currEvent.event == Event.PROJ_MOVE){
                        client.moveMissile();
                    }else{
                        throw new UnsupportedOperationException();
                    }
                    
                    if(!seenSet){
                       seenEvent = currEvent;
                       seenSet = true;
                    }
                                   
                    gQueue.add(currEvent);  //Reinsert events that are not yours
                }
                
                //Process the client's local queue
                Client me = clientTable.get(myPlayer.name);
                //After this, insert the client's local queue into the global queue.
                while(!mEventQueue.isEmpty()){
                    currEvent = mEventQueue.take();
                    if(currEvent.event == Event.UP){
                        me.forward();
                    }else if(currEvent.event == Event.DOWN){
                        me.backup();
                    }else if(currEvent.event == Event.LEFT){
                        me.turnLeft();
                    }else if(currEvent.event == Event.RIGHT){
                        me.turnRight();
                    }else if(currEvent.event == Event.FIRE){
                        me.fire();
                    }else if(currEvent.event == Event.PROJ_MOVE){
                        me.moveMissile();
                    }else{
                        throw new UnsupportedOperationException();
                    }
                    gQueue.add(currEvent);
                }
                
                //Setting this token means we are done what we want to do 
                received.incCount();
                System.out.println("Ready to Send");
                System.out.println(received);
                
                assert(mTQueue.isEmpty());
                mTQueue.put(received);
                

            }catch(IOException e){
                e.printStackTrace();
            }catch(ClassNotFoundException e){
                e.printStackTrace();
            } catch (InterruptedException ex) {
                Logger.getLogger(ClientListenerThread.class.getName()).log(Level.SEVERE, null, ex);
            }            
        }
    }
}
