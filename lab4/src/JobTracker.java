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
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.Watcher.Event.EventType;
/**
 *
 * @author wingsion
 */
public class JobTracker {
    private static String host = null;     //JobTracker hostname
    private static final int PORT_NO = 9003;   //JobTracker port
    
    //CONSTANTS
    private static final String JOB_STATUS = "status";
    
    
    //ZooKeeper members
    ZkConnector zkc = null;
    Watcher watcher = null;
    private static final String myPath = "/primary_jt";
    
    //Socket members
    ServerSocket serverSk = null;
    Socket client = null;   //ClientDriver Socket
    ObjectInputStream in = null;    //Socket input stream; receive requests
    ObjectOutputStream out = null;  //Socket output stream; write replies
    
    public JobTracker (String hosts){
       zkc = new ZkConnector();
        try {
            zkc.connect(hosts);
        } catch(Exception e) {
            System.out.println("Zookeeper connect "+ e.getMessage());
        }
        
        //At this point, zookeeper is connected
        watcher = new Watcher() { // Anonymous Watcher
            @Override
            public void process(WatchedEvent event) {
                handleEvent(event);
            } 
        };
        
    }
    
    public void determinePrimary(){
        
        assert(zkc != null);
        Stat stat = zkc.exists(myPath, watcher);
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
                    System.out.println("[Job Tracker] Accepting connections on port " + PORT_NO);
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
                        String request = (String) in.readObject();  //Receive a request
                        if(request.equals(JOB_STATUS)){
                            //If the client is requesting a status, check if the current
                            //job request 
                        }
                     }
                     catch(IOException e){
                        System.err.println("[FileServer] Failed to read data from socket");
                        System.err.println(e.getMessage());
                        System.exit(-1);
                     }
                     catch(ClassNotFoundException e){
                        System.err.println("[FileServer] Class not found");
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
