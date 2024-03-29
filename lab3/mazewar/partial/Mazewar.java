/*
Copyright (C) 2004 Geoffrey Alan Washburn
   
This program is free software; you can redistribute it and/or
modify it under the terms of the GNU General Public License
as published by the Free Software Foundation; either version 2
of the License, or (at your option) any later version.
   
This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.
   
You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307,
USA.
*/
  
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import javax.swing.BorderFactory;
import javax.swing.JFrame;
import javax.swing.JOptionPane;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.JTextPane;

/**
 * The entry point and glue code for the game.  It also contains some helpful
 * global utility methods.
 * @author Geoffrey Washburn &lt;<a href="mailto:geoffw@cis.upenn.edu">geoffw@cis.upenn.edu</a>&gt;
 * @version $Id: Mazewar.java 371 2004-02-10 21:55:32Z geoffw $
 */

public class Mazewar extends JFrame {

        /**
         * The default width of the {@link Maze}.
         */
        private final int mazeWidth = 20;

        /**
         * The default height of the {@link Maze}.
         */
        private final int mazeHeight = 10;

        /**
         * The default random seed for the {@link Maze}.
         * All implementations of the same protocol must use 
         * the same seed value, or your mazes will be different.
         */
        private final int mazeSeed = 42;

        /**
         * The {@link Maze} that the game uses.
         */
        private Maze maze = null;

        /**
         * The Mazewar instance itself. 
         */
        private Mazewar mazewar = null;
        private MSocket mSocket = null;
        private ObjectOutputStream out = null;
        private ObjectInputStream in = null;

        /**
         * The {@link GUIClient} for the game.
         */
        private GUIClient guiClient = null;
        
        
        /**
         * A map of {@link Client} clients to client name.
         */
        private Hashtable<String, Client> clientTable = null;

        /**
         * A queue of events.
         */
        private BlockingQueue eventQueue = null;
        
        /**
         * The panel that displays the {@link Maze}.
         */
        private OverheadMazePanel overheadPanel = null;

        /**
         * The table the displays the scores.
         */
        private JTable scoreTable = null;
        
        
        /******** CUSTOM MEMBERS - Lab 3 **********/
        
        private Player prevNode = null;
        private Player nextNode = null;
        
        private static String host = null;
        private static String clienthost = null;
        private static int port = -1;   //This is the server port
        
        private RingAcceptSocket rAcceptSocket = null;
        
        private BlockingQueue<Token> tQueue = null;
        
        private Player myPlayer = null;
        
        /** 
         * Create the textpane statically so that we can 
         * write to it globally using
         * the static consolePrint methods  
         */
        private static final JTextPane console = new JTextPane();
      
        /** 
         * Write a message to the console followed by a newline.
         * @param msg The {@link String} to print.
         */ 
        public static synchronized void consolePrintLn(String msg) {
                console.setText(console.getText()+msg+"\n");
        }
        
        /** 
         * Write a message to the console.
         * @param msg The {@link String} to print.
         */ 
        public static synchronized void consolePrint(String msg) {
                console.setText(console.getText()+msg);
        }
        
        /** 
         * Clear the console. 
         */
        public static synchronized void clearConsole() {
           console.setText("");
        }
        
        /**
         * Static method for performing cleanup before exiting the game.
         */
        public static void quit() {
                // Put any network clean-up code you might have here.
                // (inform other implementations on the network that you have 
                //  left, etc.)
                

                System.exit(0);
        }
       
        /** 
         * The place where all the pieces are put together. 
         */
        public Mazewar(String serverHost, int serverPort) throws IOException,
                                                ClassNotFoundException {
                super("ECE419 Mazewar");
                consolePrintLn("ECE419 Mazewar started!");
                
                // Create the maze
                maze = new MazeImpl(new Point(mazeWidth, mazeHeight), mazeSeed);
                assert(maze != null);
                
                // Have the ScoreTableModel listen to the maze to find
                // out how to adjust scores.
                ScoreTableModel scoreModel = new ScoreTableModel();
                assert(scoreModel != null);
                maze.addMazeListener(scoreModel);
                
                // Throw up a dialog to get the GUIClient name.
                String name = JOptionPane.showInputDialog("Enter your name");
                if((name == null) || (name.length() == 0)) {
                  Mazewar.quit();
                }
                
                mSocket = new MSocket(serverHost, serverPort);
                
                //Send hello packet to server..send NSPacket
                //MPacket hello = new MPacket(name, MPacket.HELLO, MPacket.HELLO_INIT);
                NSPacket hello = new NSPacket(name, NamingService.NAMING_SERVICE_STRING, name, clienthost, port);
                
                hello.mazeWidth = mazeWidth;
                hello.mazeHeight = mazeHeight;
                
                if(Debug.debug) System.out.println("Sending hello");
                mSocket.writeObject(hello);
                if(Debug.debug) System.out.println("hello sent");
                //Receive response from server
                NSPacket resp = (NSPacket)mSocket.readObject();
                if(Debug.debug) System.out.println("Received response from server");

                ArrayList<Player> players = resp.getmPlayers();
                
                //Initialize queue of events
                eventQueue = new LinkedBlockingQueue<Event>();
                
                maze.setEventQueue(eventQueue);
                
                tQueue = new LinkedBlockingQueue<Token>();
                //Initialize hash table of clients to client name 
                clientTable = new Hashtable<String, Client>(); 
                if(players.size() > 1){ //We only want to set neighbours if there's more than one client. 
                    for(int i = 0; i < resp.getmPlayers().size(); i++){
                        if(players.get(i).name.equals(name)){
                            
                            myPlayer = players.get(i);
                            if(i == 0){
                                prevNode = players.get(players.size()-1);
                                nextNode = players.get(i+1);
                            }
                            else if(i == players.size()-1){
                                prevNode = players.get(i-1);
                                nextNode = players.get(0);
                            }
                            else{
                                prevNode = players.get(i-1);
                                nextNode = players.get(i+1);
                            }
                        
                        
                            if(i == 0){
                                System.out.println("TOKEN GENERATED!");
                                
                                try{
                                    tQueue.put(new Token());
                                    maze.setName(myPlayer.name);
                                    maze.setCoordinator(true);
                                }catch (InterruptedException e){
                                    System.out.println(e);
                                }
                                
                            }
                        }
                        
                    }
                }
                
                
                rAcceptSocket = new RingAcceptSocket(myPlayer.port);    //Create an instance of the accept socket
                
                // Create the GUIClient and connect it to the KeyListener queue
                //RemoteClient remoteClient = null;
                for(Player player: resp.getmPlayers()){  
                    if(player.name.equals(name)){
                        if(Debug.debug)System.out.println("Adding guiClient: " + player);
                        guiClient = new GUIClient(name, eventQueue);
                        maze.addClient(guiClient);
                        this.addKeyListener(guiClient);
                        clientTable.put(player.name, guiClient);
                    }else{
                        if(Debug.debug)System.out.println("Adding remoteClient: " + player);
                        RemoteClient remoteClient = new RemoteClient(player.name);
                        maze.addClient(remoteClient);
                        clientTable.put(player.name, remoteClient);
                    }
                }
                
                // Use braces to force constructors not to be called at the beginning of the
                // constructor.
                /*
                {
                        maze.addClient(new RobotClient("Norby"));
                        maze.addClient(new RobotClient("Robbie"));
                        maze.addClient(new RobotClient("Clango"));
                        maze.addClient(new RobotClient("Marvin"));
                }
                */

                // Create the panel that will display the maze.
                overheadPanel = new OverheadMazePanel(maze, guiClient);
                assert(overheadPanel != null);
                maze.addMazeListener(overheadPanel);
                
                // Don't allow editing the console from the GUI
                console.setEditable(false);
                console.setFocusable(false);
                console.setBorder(BorderFactory.createTitledBorder(BorderFactory.createEtchedBorder()));
               
                // Allow the console to scroll by putting it in a scrollpane
                JScrollPane consoleScrollPane = new JScrollPane(console);
                assert(consoleScrollPane != null);
                consoleScrollPane.setBorder(BorderFactory.createTitledBorder(BorderFactory.createEtchedBorder(), "Console"));
                
                // Create the score table
                scoreTable = new JTable(scoreModel);
                assert(scoreTable != null);
                scoreTable.setFocusable(false);
                scoreTable.setRowSelectionAllowed(false);

                // Allow the score table to scroll too.
                JScrollPane scoreScrollPane = new JScrollPane(scoreTable);
                assert(scoreScrollPane != null);
                scoreScrollPane.setBorder(BorderFactory.createTitledBorder(BorderFactory.createEtchedBorder(), "Scores"));
                
                // Create the layout manager
                GridBagLayout layout = new GridBagLayout();
                GridBagConstraints c = new GridBagConstraints();
                getContentPane().setLayout(layout);
                
                // Define the constraints on the components.
                c.fill = GridBagConstraints.BOTH;
                c.weightx = 1.0;
                c.weighty = 3.0;
                c.gridwidth = GridBagConstraints.REMAINDER;
                layout.setConstraints(overheadPanel, c);
                c.gridwidth = GridBagConstraints.RELATIVE;
                c.weightx = 2.0;
                c.weighty = 1.0;
                layout.setConstraints(consoleScrollPane, c);
                c.gridwidth = GridBagConstraints.REMAINDER;
                c.weightx = 1.0;
                layout.setConstraints(scoreScrollPane, c);
                                
                // Add the components
                getContentPane().add(overheadPanel);
                getContentPane().add(consoleScrollPane);
                getContentPane().add(scoreScrollPane);
                
                // Pack everything neatly.
                pack();

                // Let the magic begin.
                setVisible(true);
                overheadPanel.repaint();
                this.requestFocusInWindow();
        }

        /*
        *Starts the ClientSenderThread, which is 
         responsible for sending events
         and the ClientListenerThread which is responsible for 
         listening for events
        */
        private void startThreads() throws IOException{
            //Accept connections on our port
            new Thread(new Runnable() {
                @Override
                public void run() {
                   try{
                        RingSocket rSocket = rAcceptSocket.accept();
                        System.out.println("Accepted");
                        new Thread(new ClientListenerThread(rSocket, clientTable, eventQueue, tQueue, myPlayer)).start();   
                   }catch(IOException e){
                       System.out.println("GG can't accept");
                   }
                }
            }).start();

            new Thread(new Runnable() {
                @Override
                public void run() {
                   try{
                        RingSocket rSendSocket = new RingSocket(nextNode.host, nextNode.port);

                        //Start a new sender thread 
                        new Thread(new ClientSenderThread(rSendSocket, tQueue, myPlayer)).start();
                   }catch(IOException e){
                       System.out.println("GG can't send connection request");
                   }
                }
            }).start();
        }
        
        /**
         * Entry point for the game.  
         * @param args Command-line arguments.
         */
        public static void main(String args[]) throws IOException,
                                        ClassNotFoundException{

             host = args[0];
             port = Integer.parseInt(args[1]);
             
             clienthost = InetAddress.getLocalHost().getHostName();
             /* Create the GUI */
             Mazewar mazewar = new Mazewar(host, port);
             mazewar.startThreads();
        }
}
