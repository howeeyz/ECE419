
import java.io.Serializable;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author wingsion
 */
public class Token implements Serializable {
    private String senderName = null;  //The previous token holder's name
    
    private int count = 0;
    
    private Queue<Event> globalQueue;     // global queue that holds all events 
    
    public Token (){
        globalQueue = new LinkedBlockingQueue<Event>();
    }
    
    public void setSenderName(String sender){
        senderName = sender;
    }
    
    public String getSenderName() {
        return senderName;
    }
    
    public Queue getGlobalQueue() {
        return globalQueue;
    }
    
    public int getCount(){
        return count;
    }
    
    public void incCount(){
        count++;
    }
}
