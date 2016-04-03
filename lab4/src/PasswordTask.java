/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

import java.io.Serializable;
import java.lang.String;

/**
 *
 * @author wingsion
 */

public class PasswordTask implements Serializable{
    private String hashString;
    private int partID;
    
    public PasswordTask(){
        hashString = null;
        partID = -1;
    }
    
    public PasswordTask(String hash, int pID){
        hashString = hash;
        partID = pID;
    }

    public String getHashString() {
        return hashString;
    }

    public int getPartID() {
        return partID;
    }

    public void setHashString(String hashString) {
        this.hashString = hashString;
    }

    public void setPartID(int partID) {
        this.partID = partID;
    }
}
