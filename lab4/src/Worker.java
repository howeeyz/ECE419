
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
import java.util.Collections;
import java.util.List;
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
import org.apache.zookeeper.data.ACL;

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
    private static final String worker_path = "/workers";
    private static final String job_path = "/jobs";
    
    private static String my_path = null;
    
    private static final int PORT_NO = 9002;
    
    private static String host = null;
    
    private BlockingQueue eventQueue = null;
    private ZkConnector zkc = null;
    private Watcher fs_watcher;
    
    static final List<ACL> acl = Ids.OPEN_ACL_UNSAFE;
    
    //Socket member variables
    private Socket fs_socket = null;
    private Socket jt_socket = null;
    private ServerSocket serverSk = null;
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
        
        //THIS NEEDS TO CHANGE....CHECK IF IT EXISTS...THEN APPEND
//        public String create(String path,
//                     byte[] data,
//                     List<ACL> acl,
//                     CreateMode createMode)

        try{
            stat = zkc.getZooKeeper().exists(worker_path, null);

            while(stat == null){
                stat = zkc.getZooKeeper().exists(worker_path, null);
            }

            my_path = zkc.getZooKeeper().create(worker_path + "/worker", null, acl,  CreateMode.EPHEMERAL_SEQUENTIAL);
        }catch(KeeperException e){
            System.err.println("[Worker] Keeper Exception Encountered");
            System.exit(-1);
        }catch(InterruptedException e){
            System.err.println("[Worker] Interrupted Exception Encountered");
            System.exit(-1);
        }
        
        
        stat = null;
        //Connected to Zk, let's lookup the IP of file server
        stat = zkc.exists(fs_path, fs_watcher);
        
        while(null == stat){
            stat = zkc.exists(fs_path, fs_watcher);
        }
        
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
        //Keep checking if there are jobs and tasks available
        jobChecker();

    }
    
    private void jobChecker(){
        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {

                while(true){
                     try{
                        Stat stat = null;
                         
                        
                        String passHash = null;   //What hash am I checking?
                        int partID = -1;
                        
                        JobNodeData jnd = null;
                        
                        List<String> jobChildren = zkc.getZooKeeper().getChildren(job_path, null, stat);
                        
                        if(jobChildren.size() == 0){
                            continue;
                        }
                        
                        Collections.sort(jobChildren);
                        List<String> taskList;
                        
                        String task_path = null;
                        String cjob_path = null;
                        for(int i = 0; i < jobChildren.size(); i++){
                            
                            cjob_path = job_path + "/" + jobChildren.get(i);
                            
                            taskList = zkc.getZooKeeper().getChildren(cjob_path, null, stat);
                            
                            System.out.println(cjob_path);
                            
                            jnd = (JobNodeData) SerializerHelper.deserialize(zkc.getZooKeeper().getData(cjob_path, null, stat));
                            
                            System.out.println("I get here");
                            
                            if(taskList.size() == 0 || jnd.mStatus == JobNodeData.JOB_DONE){
                                System.out.println("The Job is Done...or there are no tasks!");
                                continue;
                            }
                            
                            for(int j = 0; j < taskList.size(); j++){
                                task_path = job_path + "/" + jobChildren.get(i) + "/" + taskList.get(j);
                                
                                System.out.println(task_path);
                                
                                TaskNodeData tnd = (TaskNodeData) SerializerHelper.deserialize(zkc.getZooKeeper().getData(task_path, null, stat));
                                
                                if(tnd.mOwner == null){
                                    
                                    System.out.println("No Owner for task_path");
                                    
                                    tnd.mOwner = my_path;
                                    
                                    //Set the owner on the znode appropriately
                                    stat = zkc.getZooKeeper().setData(my_path, SerializerHelper.serialize(tnd), -1);
                                    
                                    passHash = tnd.mPassHash;
                                    partID = tnd.mPartID;
                                    break;
                                }
                                
                            }
                            if(partID != -1){
                                break;
                            }
                        }
                        
                        if(partID == -1){
                            continue;
                        }
                     
                        
                        PasswordTask task = new PasswordTask(passHash, partID);
                        
                        System.out.println(passHash + "----" + partID);
                        
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
                            System.out.println(word);
                            String hash_word = MD5Test.getHash(word);
                            if(hash_word.equals(MD5Test.getHash(task.getHashString()))){
                                System.out.println("Word Found! " + word + " hashes to " + hash_word);
                                found = true;
                                //Set the found stuff...
                                
                                jnd.mStatus = JobNodeData.JOB_DONE;
                                jnd.mFound = true;
                                jnd.mResultString = word;
                                
                                stat = zkc.getZooKeeper().setData(cjob_path, SerializerHelper.serialize(jnd), -1);
                                break;
                            }
                        }

                        if(!found){
                            //Set the not found stuff...
                            System.out.println("Word Not Found!");
                        }
                        
                        //Time to delete the task
                        
                        zkc.getZooKeeper().delete(task_path, -1);
                        
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
                     catch(KeeperException ke){
                        System.err.println("[Worker] Keeper Exception!");
                        System.err.println(ke.getMessage());
                        System.exit(-1);
                     }
                     catch(InterruptedException ie){
                        System.err.println("[Worker] Interrupted Exception!");
                        System.err.println(ie.getMessage());
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
                
                fs_out = new ObjectOutputStream(fs_socket.getOutputStream());
                fs_in = new ObjectInputStream(fs_socket.getInputStream());
                
                

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
