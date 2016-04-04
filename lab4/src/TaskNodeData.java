
import java.io.Serializable;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author wingsion
 */
public class TaskNodeData implements Serializable {
    //Constants
    public static final int JOB_IN_PROGRESS = 0;
    public static final int JOB_DONE = 1;
    
    
    public String mPassHash = null;   //What hash am I checking?
    public int mPartID = -1;
    public String mOwner = null;
    
    
    public TaskNodeData(String passHash, int partID){
        mPassHash = passHash;
        mPartID = partID;
    }   
}
