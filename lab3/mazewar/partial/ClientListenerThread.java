import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Hashtable;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ClientListenerThread implements Runnable {

    private RingSocket ringSocket  =  null;
    private Hashtable<String, Client> clientTable = null;
    private Player previous;
    private TokenWrapper mTkWrapper = null;

    public ClientListenerThread(RingSocket rSocket,
                                Hashtable<String, Client> clientTable,
                                Player prevNode,
                                TokenWrapper tkWrapper){
        this.ringSocket = rSocket;
        this.clientTable = clientTable;
        this.previous = prevNode;
        this.mTkWrapper = tkWrapper;
        if(Debug.debug) System.out.println("Instatiating ClientListenerThread");
    }

    public void run() {
        Token received = null;
        Client client = null;
        if(Debug.debug) System.out.println("Starting ClientListenerThread");
        while(true){
            try{
                received = (Token) ringSocket.readObject();
                if(null == received){
                    continue;
                }
                System.out.println(received.getCount());
                received.incCount();
                
                Thread.sleep(2000);     //Sleep for two seconds. pass it off
                
                //setting this token means we are done what we want to do 
                //
                
                System.out.println("Ready to Send");
                System.out.println(received);
                
                mTkWrapper.setToken(received);   
                
//                System.out.println("Received " + received);
//                client = clientTable.get(received.name);
//                if(received.event == Event.UP){
//                    client.forward();
//                }else if(received.event == Event.DOWN){
//                    client.backup();
//                }else if(received.event == Event.LEFT){
//                    client.turnLeft();
//                }else if(received.event == Event.RIGHT){
//                    client.turnRight();
//                }else if(received.event == Event.FIRE){
//                    client.fire();
//                }else{
//                    throw new UnsupportedOperationException();
//                }    
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
