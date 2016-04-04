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
import java.util.concurrent.ConcurrentHashMap;
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
    
    //CONSTANTS
    private static final String JOB_STATUS = "status";
    private static final int PARTITION_SIZE = 100;  //We assume always 100 partition
    
    //ZooKeeper members
    ZkConnector zkc = null;
    Watcher primary_watcher = null;
    Watcher job_watcher = null;
    Watcher task_watcher = null;
    Watcher workers_watcher = null;
    private static final String myPath = "/primary_jt";
    private static final String jobPath = "/jobs";
    private static final String workerPath = "/workers";
    
    //jobtracking members
    ConcurrentHashMap<String, String> jobMap = null;
    ConcurrentHashMap<String, String> workersMap = null;
    
    
    List<String> workersList = null;
    
    
    public JobTracker (String hosts){
       zkc = new ZkConnector();
        try {
            zkc.connect(hosts);
        } catch(Exception e) {
            System.out.println("Zookeeper connect "+ e.getMessage());
        }
        
        //Initialize hashmaps
        jobMap = new ConcurrentHashMap<String, String>(); 
        workersMap = new ConcurrentHashMap<String, String>();
        
        
        //Initialize Workers List
        workersList = new ArrayList<String>();
        
        
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
        
        workers_watcher = new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                handleWorkersEvent(event);
            }
        };
        
        task_watcher = new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                handleTaskEvent(event);
            }
        };
        
        //Check if the /jobs path exists
        Stat job_stat = zkc.exists(jobPath, null);
        if(null == job_stat){
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
        
        //Check if the /workers path already exists
        try{
            Stat worker_stat = zkc.exists(workerPath, null);
            if(null == worker_stat){
                System.out.println("Creating " + workerPath);
                Code ret = zkc.create(
                            workerPath,         // Path of znode
                            null,           //Pass in host information
                            CreateMode.PERSISTENT   // Znode type, set to EPHEMERAL.
                            );
                if (ret == Code.OK){
                    System.out.println(workerPath + " znode created!");
                    workersList = zkc.getZooKeeper().getChildren(workerPath, workers_watcher);
                }
            }
        }
        catch(KeeperException e){
            System.err.println("[JobTracker] ZooKeeper Exception");
            System.err.println(e.getMessage());
            System.exit(-1);
        }
        catch(InterruptedException e){
            System.err.println("[JobTracker] Interrupted Exception");
            System.err.println(e.getMessage());
            System.exit(-1);
        }

    }
    
    public void determinePrimary(){
        
        assert(zkc != null);
        Stat stat = zkc.exists(myPath, primary_watcher);
        if(null == stat){              // znode doesn't exist; let's try creating it
            Code ret = zkc.create(
                        myPath,         // Path of znode
                        null,           //Pass in host information
                        CreateMode.EPHEMERAL   // Znode type, set to EPHEMERAL.
                        );
            if (ret == Code.OK){
                System.out.println("I'm primary JobTracker!");
                acceptSocketConnection();
            }
        }
    }
    
    public void acceptSocketConnection(){
        Thread t = new Thread (new Runnable() {
            @Override
            public void run() {
                
                ObjectInputStream in = null;
                ObjectOutputStream out = null;
                try{
                    ServerSocket serverSk = new ServerSocket(0);
                    String host_and_port = host + ":" + serverSk.getLocalPort();    //Serialize the host and port
                    System.out.println(host_and_port);
                    zkc.getZooKeeper().setData(myPath, host_and_port.getBytes(), -1);
                    Socket client = serverSk.accept();
                    acceptSocketConnection();   //Start accepting a new socket request
                    in = new ObjectInputStream(client.getInputStream());
                    out = new ObjectOutputStream(client.getOutputStream());
                }
                catch(IOException e){
                    System.err.println("[JobTracker] Failed to accept a socket connection.");
                    System.err.println(e.getMessage());
                    System.exit(-1);
                }
                catch(KeeperException e){
                    System.err.println("[JobTracker] ZooKeeper exception");
                    System.err.println(e.getMessage());
                    System.exit(-1);
                }        
                catch(InterruptedException e){
                    System.err.println("[JobTracker] Interrupted exception");
                    System.err.println(e.getMessage());
                    System.exit(-1);
                }

                while(true){
                     try{
                        JPacket packet = null;
                        while(packet == null){
                            packet = (JPacket) in.readObject();
                        }
                        
                        System.out.println("Successfully read a packet!");
                        System.out.println(packet.mPHash);
                        if(packet.mType == JPacket.STATUS){
                            //If the client is requesting a status, check if the current
                            //job request 
                            if(jobMap.containsKey(packet.mPHash)){
                                //Check the progress
                                String path = jobMap.get(packet.mPHash);
                                
                                assert(zkc != null);
                                
                                Stat stat = zkc.exists(path, job_watcher);
                                
                                if(null == stat){
                                    //Exists in hashmap but the job doesn't exist
                                    //in zookeeper
                                    packet.mStatus = JPacket.JOB_ERROR;
                                }
                                else{
                                    //job path exists in zookeeper filesystem! Parse the JobNodeData
                                    byte[] job_bytes = zkc.getZooKeeper().getData(path, null, stat);
                
                                    JobNodeData jobNode =  (JobNodeData) SerializerHelper.deserialize(job_bytes);
                                    
                                    if(jobNode.mStatus == JobNodeData.JOB_IN_PROGRESS){
                                        List<String> taskList = zkc.getZooKeeper().getChildren(path,null);
                                        if(taskList.isEmpty()){
                                            //Job Is done and we beed to set status to DONE also.
                                            //Workers have gone through all tasks and did not find the word!
                                            jobNode.mStatus = JobNodeData.JOB_DONE;
                                            zkc.getZooKeeper().setData(path, SerializerHelper.serialize(jobNode), -1);
                                        }
                                    }
                                    packet.mStatus = jobNode.mStatus;
                                    packet.mFound = jobNode.mFound;
                                    packet.mResultString = jobNode.mResultString;
                                }
                            }
                            else{
                                packet.mStatus = JPacket.JOB_ERROR;
                            }
                            out.writeObject(packet);
                        }
                        else{
                            //client sent us a new job.
                            if(jobMap.containsKey(packet.mPHash)){
                                //This job has already been processed, let's check the hashmap
                                packet.mStatus = JPacket.IN_PROGRESS;
                                out.writeObject(packet);
                                continue;
                            }
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
                                System.out.println("Task Absolute path after zkc create: " + sequential_path);
                                zkc.exists(task_path, task_watcher);    //Set a watcher to update workers hashMap
                            }
                            
                            jobMap.put(packet.mPHash, path);    //Insert new job into hashmap
                            
                            packet.mStatus = JPacket.IN_PROGRESS;
                            out.writeObject(packet);
                        }
                     }
                     catch(IOException e){
                        System.err.println("[JobTracker] Socket Closed");
                        System.err.println(e.getMessage());
                        return;
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
    
    public void handleWorkersEvent(WatchedEvent event) {
         //Check the old list of workers with the new list of workers
         //See if we lost any, meaning workers in the old list is no longer in the new list
         //Find the worker in all tasks by querying all jobs and set the owner to null.
         
        String path = event.getPath();
        EventType type = event.getType();

        if(path.equalsIgnoreCase(workerPath) && type == Watcher.Event.EventType.NodeChildrenChanged){
            try{
                List<String> newWorkerList = zkc.getZooKeeper().getChildren(workerPath,
                 workers_watcher);
         
                if(!newWorkerList.containsAll(workersList)){
                    // There's a missing worker in the new worker list
                    for(String worker : workersList){
                        if(!newWorkerList.contains(worker)){
                           //Check if worker exists in the hashmap
                           // If it does, find the one task it was working on
                           // If it exists and the missing worker is in fact the owner.
                           // Set owner to null.

                           if(workersMap.containsKey(worker)){
                               String taskPath = workersMap.get(worker);

                               if(null==taskPath){
                                   workersMap.remove(worker);
                                   continue;
                               }

                               Stat stat = zkc.exists(taskPath,  null);

                               if(null != stat){
                                   //The path exists with the task.
                                   TaskNodeData taskData = (TaskNodeData)SerializerHelper.deserialize(zkc.getZooKeeper().getData(taskPath, null, stat));
                                   if(taskData.mOwner.equals(worker)){
                                       taskData.mOwner = null;
                                   }

                                   zkc.getZooKeeper().setData(taskPath, SerializerHelper.serialize(taskData), -1);
                               }
                           }   
                       }
                   }
               }

               workersList = newWorkerList;    //update the old workers list. 
            }
            catch(IOException e){
              System.err.println("[JobTracker] I/O Exception");
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
    
    public void handleTaskEvent(WatchedEvent event){
        //Get the updated group of workers.
        String path = event.getPath();
        EventType type = event.getType();
        Stat stat = null;
        try{
            if (type == Watcher.Event.EventType.NodeDataChanged) {
                TaskNodeData task = (TaskNodeData) SerializerHelper.deserialize(zkc.getZooKeeper().getData(path, task_watcher, stat));

                workersMap.put(task.mOwner, path);
            }            
        }
        catch(IOException e){
          System.err.println("[JobTracker - Task Watcher] I/O Exception");
          System.err.println(e.getMessage());
          System.exit(-1);
        }
        catch(ClassNotFoundException e){
          System.err.println("[JobTracker - Task Watcher] Class not found Exception");
          System.err.println(e.getMessage());
          System.exit(-1);
        }
        catch(KeeperException e){
          System.err.println("[JobTracker - Task Watcher] KeeperException");
          System.err.println(e.getMessage());
          System.exit(-1);
        }
        catch(InterruptedException e){
          System.err.println("[JobTracker - Task Watcher] InterruptedException");
          System.err.println(e.getMessage());
          System.exit(-1);
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
