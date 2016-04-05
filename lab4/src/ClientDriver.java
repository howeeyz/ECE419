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
import java.util.List;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.data.ACL;

public class ClientDriver {
    
    private static String host = null;
    private final String jt_path = "/primary_jt";
    private final String request_path = "/requests";
    
    private String my_request_path = null;
    
    private ZkConnector zkc;
    private Watcher request_watcher;
    
    private boolean processed = false;
    
    static final List<ACL> acl = Ids.OPEN_ACL_UNSAFE;
    
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
        
        request_watcher = new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                handleRequestEvent(event);
            } 
        };
                
        Stat stat = null;
        
        stat = zkc.exists(request_path, null);
        
        while(stat == null){
            stat = zkc.exists(request_path, null);
        }
        
        JPacket jpIn = null;
        
        try{
            my_request_path = zkc.getZooKeeper().create(request_path + "/request", SerializerHelper.serialize(jp), acl, CreateMode.EPHEMERAL_SEQUENTIAL);

            stat = zkc.exists(my_request_path, request_watcher);

            while(processed == false){
                ;
            }

            jpIn = (JPacket) SerializerHelper.deserialize(zkc.getZooKeeper().getData(my_request_path, null, stat));
        }
        catch(KeeperException ke){
            System.err.println("[Client Driver] Keeper Exception!");
            System.err.println(ke.getMessage());
            System.exit(-1);
        }
        catch(InterruptedException ie){
            System.err.println("[Client Driver] Interrupted Exception!");
            System.err.println(ie.getMessage());
            System.exit(-1); 
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
    
    private void handleRequestEvent(WatchedEvent event) {
        if(event.getType() != Watcher.Event.EventType.NodeDataChanged){
            System.out.println("We should never ever ever get here!!!!!");
            return;
        }
        
        processed = true;
        return;
    }
    
}
