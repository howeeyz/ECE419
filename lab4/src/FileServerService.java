/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author zhangh55
 */
import java.io.File;
import java.lang.String;
import java.util.ArrayList;
import java.io.FileNotFoundException;
import java.io.IOException;
import static java.lang.Math.ceil;
import static java.lang.System.exit;
import java.util.Queue;
import java.util.Scanner;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.Watcher.Event.EventType;

public class FileServerService {
    
    private Queue<String> dictionary;
    private ArrayList<ArrayList<String>> allPartitions;
    
    
    private String myPath = "/boss";
    private ZkConnector zkc;
    private Watcher watcher;
    
    public FileServerService(String hosts, String fileName){
        
        dictionary = new LinkedBlockingQueue<String>();
        allPartitions = new ArrayList<ArrayList<String>>();
        
        
        zkc = new ZkConnector();
        
        try {
            zkc.connect(hosts);
        } catch(Exception e) {
            System.out.println("Zookeeper connect "+ e.getMessage());
        }
 
        try{
            Scanner s = new Scanner(new File(fileName));
            while (s.hasNext()){
                dictionary.add(s.next());
            }
            s.close();
        }catch(FileNotFoundException e){
            System.out.println("Invalid Dictionary File Received");
            exit(1);
        }
        
        int partitionSize = (int)ceil((double)dictionary.size()/100);
      
        for(int i = 0; i < 100; i++){
            
            System.out.println("**************PARITION NUMBER " + i + "********************");
            
            ArrayList<String> partition = new ArrayList<String>();
            for(int j = 0; j < partitionSize && !dictionary.isEmpty(); j++){
                partition.add(dictionary.remove());
                
                System.out.println(partition.get(partition.size() - 1));
                
            }
            allPartitions.add(partition);
        }

        
        watcher = new Watcher() { // Anonymous Watcher
                            @Override
                            public void process(WatchedEvent event) {
                                handleEvent(event);
                        
                            } };
    }
    
    public static void main(String args[]){
        
        if (args.length != 2) {
            System.out.println("Usage: java -classpath lib/zookeeper-3.3.2.jar:lib/log4j-1.2.15.jar:. FileServerService zkServer:clientPort fileName");
            return;
        }
        
        FileServerService fss = new FileServerService(args[0], args[1]);
        
        fss.determineBoss();
        
        System.out.println("Sleeping...");
        while (true) {
            try{ Thread.sleep(5000); } catch (Exception e) {}
        }

    }


    private void determineBoss() {
        Stat stat = zkc.exists(myPath, watcher);
        if (stat == null) {              // znode doesn't exist; let's try creating it
            System.out.println("Creating " + myPath);
            Code ret = zkc.create(
                        myPath,         // Path of znode
                        null,           // Data not needed.
                        CreateMode.EPHEMERAL   // Znode type, set to EPHEMERAL.
                        );
            if (ret == Code.OK){
                System.out.println("the boss!");
                
                //split dictionary...
                
                
            }
        } 
    }

    private void handleEvent(WatchedEvent event) {
        String path = event.getPath();
        EventType type = event.getType();
        if(path.equalsIgnoreCase(myPath)) {
            if (type == EventType.NodeDeleted) {
                System.out.println(myPath + " deleted! Let's go!");       
                determineBoss(); // try to become the boss
            }
            if (type == EventType.NodeCreated) {
                System.out.println(myPath + " created!");       
                try{ Thread.sleep(5000); } catch (Exception e) {}
                determineBoss(); // re-enable the watch
            }
        }
    }
    
}
