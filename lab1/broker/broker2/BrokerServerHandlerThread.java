import java.net.*;
import java.io.*;
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.util.HashMap; 
import java.util.Iterator;
import java.util.Map; 

public class BrokerServerHandlerThread extends Thread {
	
	private Socket socket = null;
	private Map<String, String> brokerMap;
	public BrokerServerHandlerThread(Socket socket, Map<String, String> inBrokerMap) {
		super("BrokerServerHandlerThread");
		this.brokerMap = inBrokerMap;
		this.socket = socket;
		System.out.println("Created new Thread to handle client");
	}

	public void run() {

		boolean gotByePacket = false;
		
		try {
			/* stream to read from client */
			ObjectInputStream fromClient = new ObjectInputStream(socket.getInputStream());
			BrokerPacket packetFromClient;
			
			/* stream to write back to client */
			ObjectOutputStream toClient = new ObjectOutputStream(socket.getOutputStream());
			

			while (( packetFromClient = (BrokerPacket) fromClient.readObject()) != null) {
				/* create a packet to send reply back to client */
				BrokerPacket packetToClient = new BrokerPacket();
				
				
				/* process message */
				/* just echo in this example */
				if(packetFromClient.type == BrokerPacket.BROKER_REQUEST) {
					packetToClient.type = BrokerPacket.BROKER_QUOTE;
					packetToClient.symbol = packetFromClient.symbol;
					System.out.println(brokerMap.get(packetToClient.symbol));
					
					if(brokerMap.get(packetToClient.symbol) == null)
						packetToClient.quote = Long.parseLong("0");
					else
						packetToClient.quote = Long.parseLong(brokerMap.get(packetToClient.symbol));
					
					//System.out.println("From Client: " + packetFromClient.message);
				
					/* send reply back to client */
					toClient.writeObject(packetToClient);
					
					/* wait for next packet */
					continue;
				}
				
				if(packetFromClient.type == BrokerPacket.EXCHANGE_ADD){
					if(brokerMap.contains(packetFromClient.symbol)){
						packetToClient.type = ERROR_SYMBOL_EXISTS;
						toClient.writeObject(packetToClient);
						continue;
					}
					
					brokerMap.put(packetFromClient.symbol, "0");
					packetToClient.type = EXCHANGE_REPLY;
					toClient.writeObject(packetToClient);
				}
				
				if(packetFromClient.type == BrokerPacket.EXCHANGE_REMOVE){
					
				}
				
				if(packetFromClient.type == BrokerPacket.EXCHANGE_UPDATE){
					
				}
				
				/* Sending an ECHO_NULL || ECHO_BYE means quit */
				if (packetFromClient.type == BrokerPacket.BROKER_NULL || packetFromClient.type == BrokerPacket.BROKER_BYE) {
					System.out.println("I'm Here!");
					gotByePacket = true;
					packetToClient = new BrokerPacket();
					packetToClient.type = BrokerPacket.BROKER_BYE;
					packetToClient.symbol = "Bye!";
					packetToClient.quote = Long.parseLong("873");
					toClient.writeObject(packetToClient);
					break;
				}
				
				/* if code comes here, there is an error in the packet */
				System.err.println("ERROR: Unknown BROKER_* packet!!");
				System.exit(-1);
			}
			
			/* cleanup when client exits */
			System.out.println("I'm Here again!");
			fromClient.close();
			toClient.close();
			socket.close();

		} catch (IOException e) {
			if(!gotByePacket)
				e.printStackTrace();
		} catch (ClassNotFoundException e) {
			if(!gotByePacket)
				e.printStackTrace();
		}
	}
}
