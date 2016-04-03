/**
 *
 * @author zhangh55
 */

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.Watcher.Event.EventType;

public class ClientDriver {
    
    private static String host = null;
    private final String  jt_path = "/primary_jt";
    
    private ZkConnector zkc;
    private Watcher jt_watcher;
    
    private Socket jt_socket = null;
    private ObjectInputStream in = null;
    private ObjectOutputStream out = null;
    
    public ClientDriver (String hosts, String type, String pHash) throws IOException, ClassNotFoundException{
        
        int packetType;
        
        if(type.equals("job"))
            packetType = JPacket.JOB;
        else
            packetType = JPacket.STATUS;
        
        JPacket jp = new JPacket(packetType, pHash, -1, false);


        zkc = new ZkConnector();
        
        try {
            zkc.connect(hosts);
        } catch(Exception e) {
            System.out.println("[Client] Zookeeper connect "+ e.getMessage());
        }
        
        jt_watcher = new Watcher() { // Anonymous Watcher
            @Override
            public void process(WatchedEvent event) {
                handleCDEvent(event);
            } 
        };
                
        //Connected to Zk, let's lookup the IP of the job tracker
        Stat stat = zkc.exists(jt_path, jt_watcher);
        if(null != stat){
            //There's an instance of the primary file server.
            try{
                stat = null;
                byte[] host_bytes = zkc.getZooKeeper().getData(jt_path, jt_watcher, stat);
                
                String host =  new String(host_bytes);
                
                System.out.println(host);

                String [] ip_and_port = host.split(":");
                
                String ip = ip_and_port[0];
                String port = ip_and_port[1];
              
                jt_socket = new Socket(ip, Integer.parseInt(port)); 
                
                System.out.println("Socket has been made...");
                
                out = new ObjectOutputStream(jt_socket.getOutputStream());
                in = new ObjectInputStream(jt_socket.getInputStream());

            }
            catch(KeeperException e){
                System.err.println("[Client] Failed to get data from znode. Keeper Exception");
                System.err.println(e.getMessage());
                System.exit(-1);
            }
            catch(InterruptedException ie){
                System.err.println("[Client] Failed to get data from znode. Interrupted Exception");
                System.err.println(ie.getMessage());
                System.exit(-1);
            }
            catch(IOException ioe){
                System.err.println("[Client] Failed to create I/O streams.");
                System.err.println(ioe.getMessage());
                System.exit(-1);
            }
        }
        else{
            System.out.println("stat is null");
        }
        
        out.writeObject(jp);
        
        //Need to take in the data
        
        JPacket jpIn;
        jpIn = (JPacket) in.readObject();
        
        while(null == jpIn){
            jpIn = (JPacket) in.readObject();
        }
        
        
        if(jpIn.mStatus == JPacket.DONE){
            if(jpIn.mFound == true)
                System.out.println("Job Complete. Word has been found");
            else
                System.out.println("Job Complete. Word has not been found");
        }
        else if(jpIn.mStatus == JPacket.IN_PROGRESS){
            System.out.println("Job is in progress.");
        }
        else if(jpIn.mStatus == JPacket.JOB_ERROR){
            System.out.println("Job Error.");
        }
        else{
            System.out.println("ERROR: No Status");
        }


        
    }
    
    public static void main(String args[]) throws IOException, ClassNotFoundException{
        
        if (args.length < 3) {
            System.out.println("Usage: java -classpath lib/zookeeper-3.3.2.jar:lib/log4j-1.2.15.jar:. ClientDriver zkServer:clientPort <job/status> <passwordHash>");
            return;
        }

        if(!args[1].equals("job") && !args[1].equals("status")){
            System.out.println("Please ensure correct usage. Enter either job or status.");
            System.out.println("Usage: java -classpath lib/zookeeper-3.3.2.jar:lib/log4j-1.2.15.jar:. ClientDriver zkServer:clientPort <job/status> <passwordHash>");
            return;
        }
        
        ClientDriver cd = new ClientDriver(args[0], args[1], args[2]);
        
        return;

    }
    
    private void handleCDEvent(WatchedEvent event) {
        if(event.getType() != Watcher.Event.EventType.NodeCreated){
            return;
        }
        
        System.out.println("I'm handling a client driver event!");
        
        Stat stat = null;
        
        stat = zkc.exists(jt_path, jt_watcher);
        
        if(null != stat){
            //There's an instance of the primary file server.
            try{
                byte[] host_bytes = zkc.getZooKeeper().getData(jt_path, jt_watcher, stat);
                
                String host =  host_bytes.toString();

                String [] ip_and_port = host.split(":");
                
                String ip = ip_and_port[0];
                String port = ip_and_port[1];
                
                
                jt_socket = new Socket(ip, Integer.getInteger(port)); 
                
                out = new ObjectOutputStream(jt_socket.getOutputStream());
                in = new ObjectInputStream(jt_socket.getInputStream());
                
                return;
            }
            catch(KeeperException e){
                System.err.println("[Worker] Failed to get data from znode. Keeper Exception");
                System.err.println(e.getMessage());
            }
            catch(InterruptedException ie){
                System.err.println("[Worker] Failed to get data from znode. Interrupted Exception");
                System.err.println(ie.getMessage());
                System.exit(-1);
            }
            catch(IOException ioe){
                System.err.println("[Worker] Failed to create I/O streams.");
                System.err.println(ioe.getMessage());
                System.exit(-1);
            }
        }
    }
}
