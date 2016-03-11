import java.io.Serializable;

public class Player implements Serializable {
    //Need these because direction is not serializable
    public final static int North = 0;
    public final static int South = 1;
    public final static int East  = 2;
    public final static int West  = 3;
    
    public Point point = null;
    public int direction;
    public String name;
    
    public String host;
    public int port;
    
    public Player(String name, Point point, int direction, String host, int port){
        this.point = point;
        this.name = name;
        this.direction = direction;
        this.host = host;
        this.port = port;
        
    }
    public String toString(){
    	return "[" + name + ": (" + point.getX() + "," + point.getY() + ")]"; 
    }

}