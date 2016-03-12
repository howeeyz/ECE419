/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author wingsion
 */
public class TokenWrapper {
    private Token mToken = null;
    
    public TokenWrapper(Token token){
        mToken = token;
    }
    
    public void clearToken(){
        mToken = null;
    }
    
    public void setToken(Token token){
        if(null != mToken){
            System.out.println("Warning, setting mToken while it still has a value");
        }
        
        mToken = token;
    }
    
    public Token getToken(){
        return mToken;
    }
}
