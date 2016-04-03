
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

    public int mType;
    public String mPHash;

    public JPacket(){
        mType = 0;
        mPHash = null;
    }
    
    public JPacket (int type, String pHash){
        mType = type;
        mPHash = pHash;
    }
    
}
