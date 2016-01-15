import java.io.*;
import java.net.*;

public class BrokerExchange {
	public static void main(String[] args) throws IOException,
			ClassNotFoundException {

		Socket echoSocket = null;
		ObjectOutputStream out = null;
		ObjectInputStream in = null;
       
		try {
			/* variables for hostname/port */
			String hostname = "localhost";
			int port = 4444;
			
			if(args.length == 2 ) {
				hostname = args[0];
				port = Integer.parseInt(args[1]);
			} else {
				System.err.println("ERROR: Invalid arguments!");
				System.exit(-1);
			}
			echoSocket = new Socket(hostname, port);

			out = new ObjectOutputStream(echoSocket.getOutputStream());
			in = new ObjectInputStream(echoSocket.getInputStream());

		} catch (UnknownHostException e) {
			System.err.println("ERROR: Don't know where to connect!!");
			System.exit(1);
		} catch (IOException e) {
			System.err.println("ERROR: Couldn't get I/O for the connection.");
			System.exit(1);
		}

		BufferedReader stdIn = new BufferedReader(new InputStreamReader(System.in));
		String userInput;

		System.out.print(">");
		while ((userInput = stdIn.readLine()) != null) {
			/* make a new request packet */
			BrokerPacket packetToServer = new BrokerPacket();
			if(userInput.equals("x")){
				/* tell server that i'm quitting */
				packetToServer.type = BrokerPacket.BROKER_BYE;
				packetToServer.symbol = "Bye!";
				out.writeObject(packetToServer);
				break;
			}
			
			String input = userInput;
			String[] elements = input.split(" ");
			
			String command = elements[0];
			String symbol = elements[1];
			String price;
			
			packetToServer.symbol = symbol;
			
			if(command.equals("update")){
				price = elements[2];
				packetToServer.type = BrokerPacket.EXCHANGE_UPDATE;
				packetToServer.quote = Long.parseLong(price);
			}
			else if(command.equals("add")){
				packetToServer.type = BrokerPacket.EXCHANGE_ADD;
			}
			else if(command.equals("remove")){
				packetToServer.type = BrokerPacket.EXCHANGE_REMOVE;
			}
				
			out.writeObject(packetToServer);

			/* print server reply */
			BrokerPacket packetFromServer;
			packetFromServer = (BrokerPacket) in.readObject();

			/* re-print console prompt */
			System.out.print(">");
		}
		
		/* print server reply */
		BrokerPacket packetFromServer;
		packetFromServer = (BrokerPacket) in.readObject();
		if (packetFromServer.type == BrokerPacket.BROKER_BYE){
			out.close();
			in.close();
			stdIn.close();
			echoSocket.close();
		}
	}
}
