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
    Watcher request_watcher = null;
    
    
    private static final String myPath = "/primary_jt";
    private static final String jobPath = "/jobs";
    private static final String workerPath = "/workers";
    private static final String requestPath = "/requests";
    
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
        
        //Set a new primary watcher.
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
        
        request_watcher = new Watcher() {
            @Override
            public void process (WatchedEvent event) {
                handleRequestEvent(event);
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
                }
            }
            //register initial watchers
            workersList = zkc.getZooKeeper().getChildren(workerPath, workers_watcher);
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
        
        //Check if the request node exists
        Stat request_stat = zkc.exists(requestPath, request_watcher);
        if(null == request_stat){
            System.out.println("Creating " + requestPath);
            Code ret = zkc.create(
                        requestPath,         // Path of znode
                        null,           //Pass in host information
                        CreateMode.PERSISTENT   // Znode type, set to EPHEMERAL.
                        );
            if (ret == Code.OK){
                System.out.println(requestPath + " znode created!");
            }
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
                //Initialize hashmaps
                initializeHashMaps();
                acceptSocketConnection();
            }
        }
    }
    
    public void acceptSocketConnection(){
        Thread t = new Thread (new Runnable() {
            @Override
            public void run() {
                while(true){
                     try{
                        JPacket packet = null;
                        
                        //Get a list of requests
                        List<String> requests = zkc.getZooKeeper().getChildren(requestPath, null);
                        Stat stat = null;
                        
                        String absolute_request_path = null;
                        for(String request : requests){
                            absolute_request_path = requestPath + "/" + request;
                            byte[] req_bytes = zkc.getZooKeeper().getData(requestPath + "/" + request, null, stat);
                            packet = (JPacket) SerializerHelper.deserialize(req_bytes);
                            if(null == packet || packet.mStatus == -1){
                                break;
                            }
                        }
                        if(null == packet){
                           continue; 
                        }
                        System.out.println(packet.mPHash);
                        if(packet.mType == JPacket.STATUS){
                            //If the client is requesting a status, check if the current
                            //job request 
                            
                            if(jobMap.containsKey(packet.mPHash)){
                                //Check the progress
                                String path = jobMap.get(packet.mPHash);
                                
                                assert(zkc != null);
                                
                                stat = zkc.exists(path, job_watcher);
                                
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
                            if(zkc.exists(absolute_request_path, null) != null){
                                zkc.getZooKeeper().setData(absolute_request_path, SerializerHelper.serialize(packet), -1);
                            }
                        }
                        else{
                            //client sent us a new job.
                            if(jobMap.containsKey(packet.mPHash)){
                                //This job has already been processed, let's check the hashmap
                                packet.mStatus = JPacket.IN_PROGRESS;
                                if(zkc.exists(absolute_request_path, null) != null){
                                    zkc.getZooKeeper().setData(absolute_request_path, SerializerHelper.serialize(packet), -1);
                                }
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
                                zkc.exists(sequential_path, task_watcher);    //Set a watcher to update workers hashMap
                            }
                            
                            jobMap.put(packet.mPHash, path);    //Insert new job into hashmap
                            
                            packet.mStatus = JPacket.IN_PROGRESS;
                            if(zkc.exists(absolute_request_path, null) != null){
                                zkc.getZooKeeper().setData(absolute_request_path, SerializerHelper.serialize(packet), -1);
                            }
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
                        continue;
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
    
    public void initializeHashMaps(){
        assert(workersMap != null && workersMap.isEmpty());
        assert(jobMap != null && jobMap.isEmpty());
        
        
        System.out.println("Initializing Hash Maps");
        
        try{
            String absolutePath = jobPath; //Start with the jobPath
            List<String> jobs = zkc.getZooKeeper().getChildren(jobPath, null);
            Stat stat = null;
            for(String job: jobs){
                absolutePath = jobPath + "/" + job;
                byte[] job_bytes = zkc.getZooKeeper().getData(absolutePath, null, stat);
                JobNodeData jobNode =  (JobNodeData) SerializerHelper.deserialize(job_bytes);
                jobMap.put(jobNode.mPassHash, absolutePath);

                List<String> tasks = zkc.getZooKeeper().getChildren(absolutePath, null);
                for(String task : tasks){
                    //Go through all tasks and see the owners.
                     absolutePath = jobPath + "/" + job + "/" + task;
                     byte[] task_bytes = zkc.getZooKeeper().getData(absolutePath, null, stat);
                     TaskNodeData taskNode =  (TaskNodeData) SerializerHelper.deserialize(task_bytes);
                     if(taskNode.mOwner != null){
                        workersMap.put(taskNode.mOwner, absolutePath); 
                     }
                }
            }
        }catch(IOException e){
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
            
            zkc.exists(path, primary_watcher);
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
        
        System.out.println(path);
        System.out.println(type);

        if(path.equalsIgnoreCase(workerPath) && type == Watcher.Event.EventType.NodeChildrenChanged){
            try{
                List<String> newWorkerList = zkc.getZooKeeper().getChildren(workerPath,
                 workers_watcher);
                for(String worker : workersList){
                    String absoluteWorkerPath = workerPath + "/" + worker;
                    if(!newWorkerList.contains(worker)){
                       //Check if worker exists in the hashmap
                       // If it does, find the one task it was working on
                       // If it exists and the missing worker is in fact the owner.
                       // Set owner to null.

                       if(workersMap.containsKey(absoluteWorkerPath)){
                           String taskPath = workersMap.get(absoluteWorkerPath);

                           if(null==taskPath){
                               continue;
                           }

                           Stat stat = zkc.exists(taskPath, null);

                           if(null != stat){
                               //The path exists with the task.
                               TaskNodeData taskData = (TaskNodeData)SerializerHelper.deserialize(zkc.getZooKeeper().getData(taskPath, null, stat));
                               if(null != taskData && null != taskData.mOwner && taskData.mOwner.equals(absoluteWorkerPath)){

                                   taskData.mOwner = null;
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
        else if(path.equalsIgnoreCase(workerPath)){
            zkc.exists(path, workers_watcher);    //Set a watcher to update workers hashMap
            return;
        }
    }
    
    public void handleTaskEvent(WatchedEvent event){
        //Get the updated group of workers.
        String path = event.getPath();
        EventType type = event.getType();
        Stat stat = null;
        if (type == Watcher.Event.EventType.NodeDeleted) {
            //Node's been deleted, we don't care anymore
            return;
        }  
        if (type != Watcher.Event.EventType.NodeDataChanged) {
            zkc.exists(path, task_watcher);    //Set a watcher to update workers hashMap
            return;
        }      
        try{
            if (type == Watcher.Event.EventType.NodeDataChanged) {
                TaskNodeData task = (TaskNodeData) SerializerHelper.deserialize(zkc.getZooKeeper().getData(path, task_watcher, stat));
                
                System.out.println(task.mOwner);

                if(workersMap.contains(task.mOwner)){
                    workersMap.replace(task.mOwner, path);
                }
                else{
                    workersMap.put(task.mOwner, path);
                }
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
    
    public void handleRequestEvent(WatchedEvent event){
        //Get the updated group of workers.
        String path = event.getPath();
        EventType type = event.getType();
        Stat stat = null;
        if (type == Watcher.Event.EventType.NodeChildrenChanged ) {
            //Node's been deleted, we don't care anymore

            return;
        }  
        //not sure if we need anything
        
        
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
