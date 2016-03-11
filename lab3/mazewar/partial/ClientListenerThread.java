import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Hashtable;

public class ClientListenerThread implements Runnable {

    private MSocket mSocket  =  null;
    private Hashtable<String, Client> clientTable = null;
    private Player previous;

    public ClientListenerThread( MSocket mSocket,
                                Hashtable<String, Client> clientTable,
                                Player prevNode){
        this.mSocket = mSocket;
        this.clientTable = clientTable;
        this.previous = prevNode;
        if(Debug.debug) System.out.println("Instatiating ClientListenerThread");
    }

    public void run() {
        Event received = null;
        Client client = null;
        if(Debug.debug) System.out.println("Starting ClientListenerThread");
        while(true){
            try{
                received = (Event) mSocket.readObject();
                System.out.println("Received " + received);
                client = clientTable.get(received.name);
                if(received.event == Event.UP){
                    client.forward();
                }else if(received.event == Event.DOWN){
                    client.backup();
                }else if(received.event == Event.LEFT){
                    client.turnLeft();
                }else if(received.event == Event.RIGHT){
                    client.turnRight();
                }else if(received.event == Event.FIRE){
                    client.fire();
                }else{
                    throw new UnsupportedOperationException();
                }    
            }catch(IOException e){
                e.printStackTrace();
            }catch(ClassNotFoundException e){
                e.printStackTrace();
            }            
        }
    }
}
