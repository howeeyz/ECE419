import java.io.Serializable;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.util.HashMap; 
import java.util.Iterator;
import java.util.Map; 

public class OnlineBroker implements Serializable {

	private static final String FILENAME = "nasdaq";
	
	private static Map<String, String> brokerMap = new HashMap<String, String>();
	
	
	public static void main(String[] args) {

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
			System.out.println(price);
			brokerMap.put(symbol, price);
		}
		
		printBrokerMap();
		
	}
	
	public static String lookupSymbol(String symbol){
		
		if(brokerMap.containsKey(symbol)){
			return brokerMap.get(symbol);
		}
		
		return 0;
	}
	
	public static void printBrokerMap(){
		Iterator it = brokerMap.entrySet().iterator();
		while(it.hasNext()){
			Map.Entry pair = (Map.Entry)it.next();
			System.out.println(pair.getKey() + " = " + pair.getValue());
		}
	}

}