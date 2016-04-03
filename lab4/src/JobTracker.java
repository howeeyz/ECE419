/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
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
/**
 *
 * @author wingsion
 */
public class JobTracker {
    private static String host = null;     //JobTracker hostname
    private static final int PORT_NO = 9003;   //JobTracker port
    
    //CONSTANTS
    private static final String JOB_STATUS = "status";
    private static final int PARTITION_SIZE = 100;  //We assume always 100 partition
    
    //ZooKeeper members
    ZkConnector zkc = null;
    Watcher primary_watcher = null;
    Watcher job_watcher = null;
    Watcher task_watcher = null;
    private static final String myPath = "/primary_jt";
    private static final String jobPath = "/jobs";
    private static final String workerPath = "/workers";
    
    
    
    //Socket members
    ServerSocket serverSk = null;
    Socket client = null;   //ClientDriver Socket
    ObjectInputStream in = null;    //Socket input stream; receive requests
    ObjectOutputStream out = null;  //Socket output stream; write replies
    
    //jobtracking members
    HashMap<String, String> jobMap = null;
    
    
    public JobTracker (String hosts){
       zkc = new ZkConnector();
        try {
            zkc.connect(hosts);
        } catch(Exception e) {
            System.out.println("Zookeeper connect "+ e.getMessage());
        }
        
        jobMap = new HashMap<String, String>(); //Initialize hashmap
        
        //At this point, zookeeper is connected
        
        //set a new primary watcher.
        primary_watcher = new Watcher() { // Anonymous Watcher
            @Override
            public void process(WatchedEvent event) {
                handleEvent(event);
            } 
        };
        
        //Set a new job watcher. 
        job_watcher = new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                handleJobEvent(event);
            }
        };
        
        Stat stat = zkc.exists(jobPath, null);
        
        if(null == stat){
            System.out.println("Creating " + jobPath);
            Code ret = zkc.create(
                        jobPath,         // Path of znode
                        null,           //Pass in host information
                        CreateMode.PERSISTENT   // Znode type, set to EPHEMERAL.
                        );
            if (ret == Code.OK){
                System.out.println(jobPath + " znode created!");
            }
        }
    }
    
    public void determinePrimary(){
        
        assert(zkc != null);
        Stat stat = zkc.exists(myPath, primary_watcher);
        if(null == stat){              // znode doesn't exist; let's try creating it
            System.out.println("Creating " + myPath);
            String host_and_port = host + ":" + PORT_NO;    //Serialize the host and port
            System.out.println(host_and_port);
            Code ret = zkc.create(
                        myPath,         // Path of znode
                        host_and_port,           //Pass in host information
                        CreateMode.EPHEMERAL   // Znode type, set to EPHEMERAL.
                        );
            if (ret == Code.OK){
                System.out.println("I'm primary JobTracker!");
                acceptSocketConnection();
            }
        }
    }
    
    public void acceptSocketConnection(){
        //Create an instance of server socket and accept connection
        try{
            if(null == serverSk){
                serverSk = new ServerSocket(PORT_NO);
            }
        }
        catch(IOException e){
            System.err.println("[JobTracker] Failed to listen on port " + PORT_NO);
            System.err.println(e.getMessage());
            System.exit(-1);
        }
        
        Thread t = new Thread (new Runnable() {
            @Override
            public void run() {
                try{
                    System.out.println("[JobTracker] Accepting connections on port " + PORT_NO);
                    client = serverSk.accept();
                }
                catch(IOException e){
                    System.err.println("[JobTracker] Failed to accept a socket connection.");
                    System.err.println(e.getMessage());
                    System.exit(-1);
                }
                try{
                    in = new ObjectInputStream(client.getInputStream());
                    out = new ObjectOutputStream(client.getOutputStream());
                } catch (IOException e) {
                    System.err.println(" [JobTracker] Failed to initialize I/O streams");
                    System.exit(-1);
                }
                
                while(true){
                     try{
                        JPacket packet = null;
                        while(packet == null){
                            packet = (JPacket) in.readObject();
                        }
                        if(packet.mType == JPacket.STATUS){
                            //If the client is requesting a status, check if the current
                            //job request 
                            if(jobMap.containsKey(packet.mPHash)){
                                //Check the progress
                                String path = jobMap.get(packet.mPHash);
                                
                                assert(zkc != null);
                                
                                Stat stat = zkc.exists(path, job_watcher);
                                
                                if(null == stat){
                                    //Doesn't exist, remove from map and 
                                    jobMap.remove(packet.mPHash);
                                    packet.mStatus = JPacket.DONE; //set done to true
                                }
                                else{
                                    //zkc exists
                                    byte[] job_bytes = zkc.getZooKeeper().getData(path, null, stat);
                
                                    String job =  new String(job_bytes);

                                    System.out.println(job);

                                    String [] status_and_hash = job.split(":");

                                    String status = status_and_hash[0];
  
                                    if(status.equals("1")){
                                        packet.mStatus = JPacket.DONE;
                                    }
                                    else{
                                        packet.mStatus = JPacket.IN_PROGRESS;
                                    }
                                }
                            }
                            else{
                                packet.mStatus = JPacket.DONE;
                            }
                            out.writeObject(packet);
                        }
                        else{
                            //client sent us a new job.
                            String new_job_path = jobPath + "/" + "job";
                            System.out.println("New absolute path of job: " + new_job_path);
                            JobNodeData data = new JobNodeData(packet.mPHash);
                            List<ACL> acl = Ids.OPEN_ACL_UNSAFE;
                            String path = zkc.getZooKeeper().create(new_job_path, SerializerHelper.serialize(data), acl, CreateMode.PERSISTENT_SEQUENTIAL);
                            System.out.println("Absolute path after zkc create: " + path);
                            
                            //Create Tasks.
                            
                            for(int i = 0; i < PARTITION_SIZE; i++){
                                //Create a new task
                                String task_path = path + "/task";
                                TaskNodeData taskData = new TaskNodeData(packet.mPHash, i);
                                String sequential_path = zkc.getZooKeeper().create(task_path, SerializerHelper.serialize(taskData), acl, CreateMode.PERSISTENT_SEQUENTIAL);
                            }
                            
                            jobMap.put(packet.mPHash, path);    //Insert new job into hashmap
                        }
                     }
                     catch(IOException e){
                        System.err.println("[JobTracker] Failed to read data from socket.");
                        System.err.println(e.getMessage());
                        System.exit(-1);
                     }
                     catch(ClassNotFoundException e){
                        System.err.println("[JobTracker] Class not found.");
                        System.err.println(e.getMessage());
                        System.exit(-1);
                     }
                     catch(KeeperException e){
                        System.err.println("[JobTracker] KeeperException");
                        System.err.println(e.getMessage());
                        System.exit(-1);
                     }
                     catch(InterruptedException e){
                        System.err.println("[JobTracker] InterruptedException");
                        System.err.println(e.getMessage());
                        System.exit(-1);
                     }
                }
            }
        });

        t.start();  //Start the new background thread
    }
    
    public void handleEvent(WatchedEvent event){
        //Check if workers go down,
        //Get the updated group of workers.
        String path = event.getPath();
        EventType type = event.getType();
        if(path.equalsIgnoreCase(myPath)) {
            if (type == EventType.NodeDeleted) {
                System.out.println(myPath + " deleted! Let's go!");       
                determinePrimary(); // try to become the boss
            }
            if (type == EventType.NodeCreated) {
                System.out.println(myPath + " created!");       
                try{ Thread.sleep(5000); } catch (Exception e) {}
                determinePrimary(); // re-enable the watch
            }
        }
    }
    
        public void handleJobEvent(WatchedEvent event){
        //Check if workers go down,
        //Get the updated group of workers.
        String path = event.getPath();
        EventType type = event.getType();
        if(path.equalsIgnoreCase(myPath)) {
            if (type == EventType.NodeDeleted) {
                System.out.println(myPath + " deleted! Let's go!");       
                determinePrimary(); // try to become the boss
            }
            if (type == EventType.NodeCreated) {
                System.out.println(myPath + " created!");       
                try{ Thread.sleep(5000); } catch (Exception e) {}
                determinePrimary(); // re-enable the watch
            }
        }
    }
    
    public static void main (String [] args){
        if (args.length < 1) {
            System.out.println("Usage: java -classpath lib/zookeeper-3.3.2.jar:lib/log4j-1.2.15.jar:. JobTracker zkServer:clientPort");
            return;
        }

        try{
            host = InetAddress.getLocalHost().getHostName();    //get the hostname of local machine
            JobTracker jt = new JobTracker(args[0]);    //create an instance of job tracker
            jt.determinePrimary();
        }
        catch(UnknownHostException e){
            System.err.println("[JobTracker] The host is unknown.");
            System.err.println(e.getMessage());
            System.exit(-1);
        }

        System.out.println("Sleeping...");
        while (true) {
            try{ Thread.sleep(5000); } catch (Exception e) {}
        }
    }
}
