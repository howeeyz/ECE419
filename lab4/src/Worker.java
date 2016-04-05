
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
import java.util.concurrent.TimeUnit;
import java.security.SecureRandom;
import java.util.Random;

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

    private ZkConnector zkc = null;
    private Watcher fs_watcher;
    
    private boolean fsDown = true;
    
    static final List<ACL> acl = Ids.OPEN_ACL_UNSAFE;

    
    public Worker(String hosts) {
       
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
        
        //System.out.println("Worker Node created");
        
        stat = null;
        //Connected to Zk, let's lookup the IP of file server
        stat = zkc.exists(fs_path, fs_watcher);
        
        while(null == stat){
            stat = zkc.exists(fs_path, fs_watcher);
        }
        
        fsDown = false;
        
        //System.out.println("FS Node created");
       
        //fsConnect();
       
        //Keep checking if there are jobs and tasks available
        jobChecker();

    }
    
    ArrayList<String> partitionGetter(PasswordTask task){
        Stat stat = null;
        ArrayList<String> partition = null;
        
        try{
            stat = null;
            byte[] host_bytes = zkc.getZooKeeper().getData(fs_path, fs_watcher, stat);
            while(host_bytes == null){
                host_bytes = zkc.getZooKeeper().getData(fs_path, fs_watcher, stat);
            }
            String host =  new String(host_bytes);

            //System.out.println(host);

            String [] ip_and_port = host.split(":");

            String ip = ip_and_port[0];
            String port = ip_and_port[1];

            Socket fs_socket = new Socket(ip, Integer.parseInt(port)); 
            //System.out.println("Created the sockets");
            ObjectOutputStream fs_out = new ObjectOutputStream(fs_socket.getOutputStream());
            //System.out.println("Created output stream");
            ObjectInputStream fs_in = new ObjectInputStream(fs_socket.getInputStream());
            //System.out.println("Created input stream");

            
            //System.out.println("Created the sockets, the input and output streams");
            
            //Send over the partition ID

            assert(fs_socket != null);
            fs_out.writeObject(task);
            
            //Get the partition
            while(partition == null){
               partition = (ArrayList<String>)fs_in.readObject(); 
            }
            
            //Close the socket
            fs_socket.close();
            
            //Return the partition
            
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
            //System.err.println("[Worker] Failed to create I/O streams.");
            //System.err.println(ioe.getMessage());
            return null;
        }
        catch(ClassNotFoundException cnf){
            System.err.println("[Worker] ClassNotFound");
            System.err.println(cnf.getMessage());
            System.exit(-1);
        }

        return partition;
    }
    
    public static int randInt(int min, int max) {
        Random rand = new Random();
        int randomNum = rand.nextInt((max - min) + 1) + min;
        return randomNum;
    }
    
    private void jobChecker(){
        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {

                while(true){
                     try{
                        Stat stat = null;
                        String passHash = null;   //What hash am I checking?
                        int partID = -1;    //What partition ID do I check
                        
                        JobNodeData jnd = null;
                        
                        List<String> jobChildren = zkc.getZooKeeper().getChildren(job_path, null, stat);
                        
                        if(jobChildren.size() == 0){
                            continue;
                        }
                        
                        Collections.sort(jobChildren);
                        List<String> taskList = null;
                        
                        String task_path = null;
                        String cjob_path = null;
                        for(int i = 0; i < jobChildren.size(); i++){
                            
                            cjob_path = job_path + "/" + jobChildren.get(i);
                            
                            taskList = zkc.getZooKeeper().getChildren(cjob_path, null, stat);
                                                        
                            jnd = (JobNodeData) SerializerHelper.deserialize(zkc.getZooKeeper().getData(cjob_path, null, stat));
                            
                            if(jnd.mStatus == JobNodeData.JOB_DONE || taskList.isEmpty()){
                                continue;
                            }
                            
                            //There are tasks that still exist
                            for(int j = 0; j < taskList.size(); j++){
                                task_path = cjob_path + "/" + taskList.get(j);
                                
                                //System.out.println(task_path);
                                
                                TaskNodeData tnd = (TaskNodeData) SerializerHelper.deserialize(zkc.getZooKeeper().getData(task_path, null, stat));
                                
                                if(tnd.mOwner == null || tnd.mOwner.equals(my_path)){
                                    
                                    if(tnd.mOwner == null){
                                        tnd.mOwner = my_path;
                                        //Set the owner on the znode appropriately
                                        stat = zkc.getZooKeeper().setData(task_path, SerializerHelper.serialize(tnd), -1);
                                    }
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
                        
                        //System.out.println(passHash + "----" + partID);
                        
                        while(fsDown){
                            Thread.sleep(randInt(1000, 5000));//spin
                        }
                        
                        ArrayList<String> partition = partitionGetter(task);
                        
                        if(partition == null){
                            continue;
                        }
                        
                        //partition exists here.
                        boolean found = false;
                        for(String word : partition){
                            String hash_word = MD5Test.getHash(word);
                            if(hash_word.equals(task.getHashString())){
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
                        //Time to delete the task
                        stat = zkc.exists(task_path, null);
                        
                        if(stat != null){
                            zkc.getZooKeeper().delete(task_path, -1);
                        }
                        
                     }
                     catch(IOException e){
                        System.err.println("[Worker] Failed to read data from socket");
                        System.err.println(e.getMessage());
                        continue;
                     }
                     catch(ClassNotFoundException ce){
                        System.err.println("[Worker] Class doesn't exist");
                        System.err.println(ce.getMessage());
                        System.exit(-1);
                     }
                     catch(KeeperException ke){
                        //System.err.println("[Worker] Keeper Exception!");
                        //System.err.println(ke.getMessage());
                        continue;
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
      
        if(event.getType() == Watcher.Event.EventType.NodeDeleted){
            //System.out.println("Setting fsDown to true!");
            fsDown = true;
            zkc.exists(fs_path, fs_watcher);
            return;
        }

        if(event.getType() != Watcher.Event.EventType.NodeCreated){
            zkc.exists(fs_path, fs_watcher);
            return;
        }
        //System.out.println("Setting fsDown to false!");
        fsDown = false;
        
        //System.out.println("Node created watcher event");
        Stat stat = null;
        
        stat = zkc.exists(fs_path, null);
        if(null == stat){
            return;
        }
        
        zkc.exists(fs_path, fs_watcher);
    }
    
    public static void main(String args[]) throws UnknownHostException{
        
        if (args.length != 1) {
            System.out.println("Usage: java -classpath lib/zookeeper-3.3.2.jar:lib/log4j-1.2.15.jar:. Worker zkServer:clientPort");
            return;
        }
        
        Worker worker = new Worker(args[0]);
        
        System.out.println("Sleeping...");
        while (true) {
            try{ Thread.sleep(5000); } catch (Exception e) {}
        }
    }
}
