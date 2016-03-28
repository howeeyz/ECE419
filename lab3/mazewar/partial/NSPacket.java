
import java.io.Serializable;
import java.util.ArrayList;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author wingsion
 */
public class NSPacket implements Serializable{
    private int mAckNo = -1;
    private int mSeqNo = 0;
    
    private String mHost;
    private int mPort;
    
    private String mSender;
    private String mReceiver;
    private String mPlayerName;

    //These are used to initialize the board
    public int mazeSeed;
    public int mazeHeight;
    public int mazeWidth; 
    private ArrayList<Player> mPlayers;
    
    
    public NSPacket(){
        mSender = null;
        mReceiver = null;
        mPlayerName = null;
        mHost = null;
        mPort = -1;
    }
    
    //Client sender packet constructor 
    public NSPacket(String sender, String receiver, String playerName, String host, int port){
        mSender = sender;
        mReceiver = receiver;
        mPlayerName = playerName;
        mHost = host;
        mPort = port;
    }
    
    //Server sender packet constructor
    public NSPacket(String sender, String receiver, ArrayList<Player> players){
        mSender = sender;
        mReceiver = receiver;
        mPlayers = players;
    }
    
    //Ack Constructor
    public NSPacket(int ackNo){
        mAckNo = ackNo;
    }
    
    public void setSeqNo(int seqNo){
        mSeqNo = seqNo;
    }
    
    public void setAckNo(int ackNo){
        mAckNo = ackNo;
    }

    public int getmAckNo() {
        return mAckNo;
    }

    public int getmSeqNo() {
        return mSeqNo;
    }

    public String getmSender() {
        return mSender;
    }

    public String getmReceiver() {
        return mReceiver;
    }

    public String getmPlayerName() {
        return mPlayerName;
    }

    public ArrayList<Player> getmPlayers() {
        return mPlayers;
    }

    public String toStringClient(){
        String retString = String.format("NSPACKET(FROM: %s, TO: %s, Name: %s, SEQNO: %d)", mSender, mReceiver, mPlayerName, mSeqNo);
        return retString;
    }
    
    public String toStringServer(){
        String retString = "NSPACKET(FROM: " + mSender + ", TO: " + mReceiver + ", Name: " + mPlayerName + ", SEQNO: " + mSeqNo + ")\n";
        for(int i = 0; i < mPlayers.size(); i++){
            retString = retString + "Name: " + mPlayers.get(i).name + ", " 
                                  + "Coordinates: (" + mPlayers.get(i).point.getX() + "," + mPlayers.get(i).point.getY() 
                                  + "), Direction: " + mPlayers.get(i).direction 
                                  + " Host: " + mPlayers.get(i).host 
                                  + " Port: " + mPlayers.get(i).port + "\n";
        }
        
        return retString;
    }
    
    public String toStringAck(){
        String retString = String.format("NSPACKET(FROM: %s, TO: %s, ACKNO: %d)", mSender, mReceiver, mAckNo);
        return retString;
    }
    
    public String getHost(){
        return mHost;
    }
    
    public int getPort(){
        return mPort;
    }
    
}
