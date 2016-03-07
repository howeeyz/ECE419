
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
    }
    
    //Client sender packet constructor 
    public NSPacket(String sender, String receiver, String playerName){
        mSender = sender;
        mReceiver = receiver;
        mPlayerName = playerName;
    }
    
    //Server sender packet constructor
    public NSPacket(String sender, String receiver, ArrayList<Player> players){
        mSender = sender;
        mReceiver = receiver;
        mPlayers = players;
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

}
