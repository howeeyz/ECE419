/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author zhangh55
 */
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.lang.String;
import java.util.ArrayList;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import static java.lang.Math.ceil;
import static java.lang.System.exit;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.Charset;
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

public class FileServer {
    private static String host = null;
    
    private Queue<String> dictionary;
    private ArrayList<ArrayList<String>> allPartitions;
    
    
    private String myPath = "/primary_fs";
    
    private ZkConnector zkc;
    private Watcher watcher;
    
    
    public FileServer(String hosts, String fileName){
        
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
            ArrayList<String> partition = new ArrayList<String>();
            for(int j = 0; j < partitionSize && !dictionary.isEmpty(); j++){
                partition.add(dictionary.remove());
            }
            allPartitions.add(partition);
        }

        watcher = new Watcher() { // Anonymous Watcher
            @Override
            public void process(WatchedEvent event) {
                handleEvent(event);
            } };
    }
    private void determineBoss() {
        Stat stat = zkc.exists(myPath, watcher);
        if (stat == null) {              // znode doesn't exist; let's try creating it
            System.out.println("Creating " + myPath);

            Code ret = zkc.create(
                        myPath,         // Path of znode
                        null,           //Pass in host information
                        CreateMode.EPHEMERAL   // Znode type, set to EPHEMERAL.
                        );
            if (ret == Code.OK){
                System.out.println("I'm primary!");
                acceptSocketConnection();
            }
        } 
    }
    
    private void acceptSocketConnection(){
        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                ObjectOutputStream out = null;
                ObjectInputStream in = null;
                try{
                    ServerSocket serverSk = new ServerSocket(0);
                    String host_and_port = host + ":" + serverSk.getLocalPort();    //Serialize the host and port
                    System.out.println(host_and_port);
                    zkc.getZooKeeper().setData(myPath, host_and_port.getBytes(), -1);
                    Socket client = serverSk.accept();
                    acceptSocketConnection();   //Start accepting a new socket request
                    out = new ObjectOutputStream(client.getOutputStream());
                    in = new ObjectInputStream(client.getInputStream());
                } catch (IOException e) {
                  System.out.println("Read failed");
                  System.exit(-1);
                }
                catch(KeeperException e){
                    System.err.println("[FileServer] ZooKeeper exception");
                    System.err.println(e.getMessage());
                    System.exit(-1);
                }        
                catch(InterruptedException e){
                    System.err.println("[FileServer] Interrupted exception");
                    System.err.println(e.getMessage());
                    System.exit(-1);
                }

                while(true){
                    try{
                        PasswordTask task =(PasswordTask) in.readObject();

                        ArrayList<String> partition = allPartitions.get(task.getPartID());
                        out.writeObject(partition);   //get the partition that the worker requested and send to them
                    }
                    catch(IOException e){
                        System.err.println("[FileServer] Socket closed. Accept new one");
                        return;
                    }
                    catch(ClassNotFoundException e){
                        System.err.println("[FileServer] Class not found");
                        System.err.println(e.getMessage());
                        System.exit(-1);
                    }
                }
            }
        });
        
        t.start();
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
            
            zkc.exists(path, watcher);
        }
    }
    
    public static void main(String args[]) throws IOException{
        
        if (args.length < 1) {
            System.out.println("Usage: java -classpath lib/zookeeper-3.3.2.jar:lib/log4j-1.2.15.jar:. FileServer zkServer:clientPort <fileName>");
            return;
        }
        String filename = "md5/dictionary/lowercase.rand";
        if(args.length == 2){
            filename = args[1];
        }
        
        host = InetAddress.getLocalHost().getHostName();
        FileServer fss = new FileServer(args[0], filename);
        
        fss.determineBoss();
        
        System.out.println("Sleeping...");
        while (true) {
            try{ Thread.sleep(5000); } catch (Exception e) {}
        }
    }
}
