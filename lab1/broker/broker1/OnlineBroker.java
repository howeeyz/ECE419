import java.net.*;
import java.io.*;
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.util.HashMap; 
import java.util.Iterator;
import java.util.Map; 

public class OnlineBroker implements Serializable {

	private static final String FILENAME = "nasdaq";
	
	private static Map<String, String> brokerMap = new HashMap<String, String>();
	
	
	public static void main(String[] args) throws IOException{

		ServerSocket serverSocket = null;
        boolean listening = true;
		
        try {
        	if(args.length == 1) {
        		serverSocket = new ServerSocket(Integer.parseInt(args[0]));
        	} else {
        		System.err.println("ERROR: Invalid arguments!");
        		System.exit(-1);
        	}
        } catch (IOException e) {
            System.err.println("ERROR: Could not listen on port!");
            System.exit(-1);
        }
        
		List<String> records = new ArrayList<String>();
		
		try {
			BufferedReader reader = new BufferedReader(new FileReader(FILENAME));
			String line;
			while ((line = reader.readLine()) != null) {
				records.add(line);
			}
			reader.close();
		} 
		catch (Exception e) {
			System.err.format("Exception occurred trying to read '%s'.",
					FILENAME);
			e.printStackTrace();
		}
		
		for (int x = 0; x < records.size(); x++){
			String input = records.get(x);
			String[] elements = input.split(" ");
			String symbol = elements[0];
			String price = elements[1];
			//System.out.println(price);
			brokerMap.put(symbol, price);
		}
		printBrokerMap();
		while (listening) {
			new BrokerServerHandlerThread(serverSocket.accept(), brokerMap).start();
			listening = false;
		}
		
		serverSocket.close();
		
	}
	
	public static String lookupSymbol(String symbol){
		
		if(brokerMap.containsKey(symbol)){
			return brokerMap.get(symbol);
		}
		
		return "0";
	}
	
	public static void printBrokerMap(){
		Iterator it = brokerMap.entrySet().iterator();
		while(it.hasNext()){
			Map.Entry pair = (Map.Entry)it.next();
			System.out.println(pair.getKey() + " = " + pair.getValue());
		}
	}

}