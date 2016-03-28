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
import static java.lang.System.exit;
import java.util.Scanner;

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
    
    private ArrayList<String> dictionary;
    
    private String myPath = "/boss";
    private ZkConnector zkc;
    private Watcher watcher;
    
    public FileServerService(String hosts, String fileName){
        
        dictionary = new ArrayList<String>();
        
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
                System.out.println(dictionary.get(dictionary.size() - 1));
            }
            s.close();
        }catch(FileNotFoundException e){
            System.out.println("Invalid Dictionary File Received");
            exit(1);
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
        
        //t.checkpath();
        
        System.out.println("Sleeping...");
        while (true) {
            try{ Thread.sleep(5000); } catch (Exception e) {}
        }

    }


    private void checkpath() {
        Stat stat = zkc.exists(myPath, watcher);
        if (stat == null) {              // znode doesn't exist; let's try creating it
            System.out.println("Creating " + myPath);
            Code ret = zkc.create(
                        myPath,         // Path of znode
                        null,           // Data not needed.
                        CreateMode.EPHEMERAL   // Znode type, set to EPHEMERAL.
                        );
            if (ret == Code.OK) System.out.println("the boss!");
        } 
    }

    private void handleEvent(WatchedEvent event) {
        String path = event.getPath();
        EventType type = event.getType();
        if(path.equalsIgnoreCase(myPath)) {
            if (type == EventType.NodeDeleted) {
                System.out.println(myPath + " deleted! Let's go!");       
                checkpath(); // try to become the boss
            }
            if (type == EventType.NodeCreated) {
                System.out.println(myPath + " created!");       
                try{ Thread.sleep(5000); } catch (Exception e) {}
                checkpath(); // re-enable the watch
            }
        }
    }
    
}
