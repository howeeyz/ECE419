
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
public class JobNodeData implements Serializable {
    //Constants
    public static final int JOB_DONE = 1;
    public static final int  JOB_IN_PROGRESS= 2;
    public static final int JOB_ERROR = 3;
    
    
    public static String mPassHash = null;   //What hash am I checking?
    public static int mStatus = JOB_IN_PROGRESS;  //Is this job done?
    public static boolean mFound = false;    //Did I find my result?
    public static String mResultString = null;  //What was the cleartext result
    
    
    public JobNodeData(String passHash){
        mPassHash = passHash;
    }    
}
