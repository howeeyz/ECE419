
import java.io.Serializable;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author zhangh55
 */
public class JPacket implements Serializable{
    
    /*Use this to differentiate between job and status*/
    public static final int JOB = 100;
    public static final int STATUS = 200;
    
    public static final int DONE = 1;
    public static final int IN_PROGRESS = 2;
    public static final int JOB_ERROR = 3;
    
    public int mStatus;

    public int mType;
    public String mPHash;

    public JPacket(){
        mType = 0;
        mPHash = null;
        mStatus = -1;
    }
    
    public JPacket (int type, String pHash, int status){
        mType = type;
        mPHash = pHash;
        mStatus = status;
    }

    
}
