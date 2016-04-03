
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.net.Inet4Address;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

//Zookeeper libs
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.Watcher.Event.EventType;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author wingsion
 */
public class Worker {
    //Wait to get a request from the Job Tracker
    //When it gets request from Zookeeper, it does a look up for the IP of fileserver 
    //from the primary file server we get the requested partition 
    //check if there's a hash match in the partition
    
    //CONSTANTS
    private static final String fs_path = "/primary_fs";
    private static final String my_path = "/worker";
    private static final int PORT_NO = 9002;
    
    private static String host = null;
    
    private BlockingQueue eventQueue = null;
    private ZkConnector zkc = null;
    private Watcher fs_watcher;
    
    //Socket member variables
    private Socket fs_socket = null;
    private Socket jt_socket = null;
    private ServerSocket serverSk = null;
    private ObjectInputStream in = null;
    private ObjectOutputStream out = null;
    private ObjectInputStream fs_in = null;
    private ObjectOutputStream fs_out = null;
    
    
    public Worker(String hosts) {
        eventQueue = new LinkedBlockingQueue();
        try{
            serverSk = new ServerSocket(PORT_NO);
        }
        catch(IOException e){
            System.err.println("[Worker] Failed to listen on port " + PORT_NO);
            System.err.println(e.getMessage());
            System.exit(-1);
        }
       
        fs_watcher = new Watcher() { // Anonymous Watcher
            @Override
            public void process(WatchedEvent event) {
                //Do something here
                //Node gets killed
                //data of the node changes.
                handleFSEvent(event);
            }
        };
        
        zkc = new ZkConnector();
        
        try {
            zkc.connect(hosts);
        } catch(Exception e) {
            System.out.println("Zookeeper connect "+ e.getMessage());
        }
        
        Stat stat = null;
        
        String host_and_port = host + ":" + PORT_NO;
        Code ret = zkc.create(my_path, host_and_port, CreateMode.EPHEMERAL_SEQUENTIAL);
                
        if (ret == Code.OK){
                System.out.println("Created my worker znode");
        }
        
        //Connected to Zk, let's lookup the IP of file server
        stat = zkc.exists(fs_path, fs_watcher);
        if(null != stat){
            //There's an instance of the primary file server.
            try{
                stat = null;
                byte[] host_bytes = zkc.getZooKeeper().getData(fs_path, fs_watcher, stat);
                
                String host =  new String(host_bytes);
                
                System.out.println(host);

                String [] ip_and_port = host.split(":");
                
                String ip = ip_and_port[0];
                String port = ip_and_port[1];
                
                fs_socket = new Socket(ip, Integer.parseInt(port)); 
                
                fs_out = new ObjectOutputStream(fs_socket.getOutputStream());
                fs_in = new ObjectInputStream(fs_socket.getInputStream());

            }
            catch(KeeperException e){
                System.err.println("[Worker] Failed to get data from znode. Keeper Exception");
                System.err.println(e.getMessage());
                System.exit(-1);
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
            //Accept Socket connection from Job Tracker Service.
            acceptSocketConnection(host_and_port);
        }

        
    }
    
    private void acceptSocketConnection(String hosts){
        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                try{
                    jt_socket = serverSk.accept();
                }
                catch(IOException e){
                    System.err.println("[FileServer] Failed to accept a socket connection.");
                    System.err.println(e.getMessage());
                    System.exit(-1);
                }

                try{
                    in = new ObjectInputStream(jt_socket.getInputStream());
                    out = new ObjectOutputStream(jt_socket.getOutputStream());
                } catch (IOException e) {
                    System.out.println("Read failed");
                    System.exit(-1);
                }

                while(true){
                     try{
                        PasswordTask task = (PasswordTask) in.readObject();
                        
                        //Uncomment if you want to test file server.
                        //PasswordTask task = new PasswordTask(MD5Test.getHash("bl4h bl4h"), 0);
                        if(task != null){
                            //Go to file server and request a partition
                            assert(fs_socket != null);
                            fs_out.writeObject(task);
                            
                            ArrayList<String> partition = null;
                            
                            while(partition == null){
                               partition = (ArrayList<String>)fs_in.readObject(); 
                            }
                            
                            //partition exists here.
                            boolean found = false;
                            for(String word : partition){
                                String hash_word = MD5Test.getHash(word);
                                if(hash_word.equals(task.getHashString())){
                                    System.out.println("Word Found! " + word + " hashes to " + hash_word);
                                    found = true;
                                    out.writeObject(word);
                                    break;
                                }
                            }
                            
                            if(!found){
                                System.out.println("Word Not Found!");
                            }
                        }
                     }
                     catch(IOException e){
                        System.err.println("[Worker] Failed to read data from socket");
                        System.err.println(e.getMessage());
                        System.exit(-1);
                     }
                     catch(ClassNotFoundException ce){
                        System.err.println("[Worker] Class doesn't exist");
                        System.err.println(ce.getMessage());
                        System.exit(-1);
                     }
                }
            }
        });
        
        t.start();
    }
    
    private void handleFSEvent(WatchedEvent event){
        //Connected to Zk, let's lookup the IP of file server
        Stat stat = null;
        
        stat = zkc.exists(fs_path, fs_watcher);
        
        if(null != stat){
            //There's an instance of the primary file server.
            try{
                byte[] host_bytes = zkc.getZooKeeper().getData(fs_path, fs_watcher, stat);
                
                String host =  host_bytes.toString();

                String [] ip_and_port = host.split(":");
                
                String ip = ip_and_port[0];
                String port = ip_and_port[1];
                
                
                fs_socket = new Socket(ip, Integer.getInteger(port)); 
                
                ObjectOutputStream out = new ObjectOutputStream(fs_socket.getOutputStream());
                ObjectInputStream in = new ObjectInputStream(fs_socket.getInputStream());

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
    
    public static void main(String args[]) throws UnknownHostException{
        
        if (args.length != 1) {
            System.out.println("Usage: java -classpath lib/zookeeper-3.3.2.jar:lib/log4j-1.2.15.jar:. Worker zkServer:clientPort");
            return;
        }
        
        host = Inet4Address.getLocalHost().getHostName();
        
        Worker worker = new Worker(args[0]);
        
        System.out.println("Sleeping...");
        while (true) {
            try{ Thread.sleep(5000); } catch (Exception e) {}
        }

    }
    
    
}
