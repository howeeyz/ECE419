import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.concurrent.BlockingQueue;

public class ClientSenderThread implements Runnable {
    private final int TIMEOUT = 100;
    private RingSocket ringSocket = null;
    private BlockingQueue<Token> mTQueue = null;
    private Player myPlayer = null;
    
    private NSPacket received = null;
    private boolean acked = false;
    
    public ClientSenderThread(RingSocket rSocket,
                              BlockingQueue tQueue,
                              Player player){
        this.ringSocket = rSocket;
        this.mTQueue = tQueue;
        this.myPlayer = player;
    }
    
    private void transmitToken(){
        ringSocket.writeObject(mTQueue.peek());
        
        new Thread(new Runnable() {
            @Override
            public void run() {
                try{
                    Thread.sleep(TIMEOUT);
                    if(!acked){
                        transmitToken();
                    }
                    else{
                        acked = false;
                    }
                }catch(InterruptedException e){
                    System.out.println("Timeout exception");
                }

            }
        }).start();
    }
    
    public void run() {
        if(Debug.debug) System.out.println("Starting ClientSenderThread");
        while(true){      
            if(!mTQueue.isEmpty()){
                
                try{
                
                    System.out.println("Sending now!");
                    //This tells us that the listener is done doing what it needs to do
                    //Ready to pass off token

                    transmitToken();
                    Token tk = mTQueue.peek();

                    while(received == null){
                       received = (NSPacket)ringSocket.readObject();
                    }

                    if(tk.getCount() == received.getmAckNo()){
                        acked = true;
                    }

                    mTQueue.take();
                }catch (InterruptedException e){
                    System.out.println(e);
                }catch (IOException e){
                    System.out.println(e);
                }catch (ClassNotFoundException e){
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
