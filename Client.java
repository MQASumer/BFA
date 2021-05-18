import java.io.*;
import java.net.*;
//import java.util.*;

public class Client {

	public static class Server implements Comparable<Server> { // Server class to hold server info
		String Type = "";
		int ID;
		int core;

		public Server(String Type, String core) {
			this.Type = Type;
			this.core = Integer.parseInt(core);
		}

		public String getType() {
			return Type;
		}

		public String getCore() {
			return Integer.toString(core);
		}

		public void setType(String newType) {
			this.Type = newType;
		}

		public void setCore(String newCore) {
			this.core = Integer.parseInt(newCore);
		}

		@Override
		public int compareTo(Client.Server o) {
			// sort by core then type ascending order.
			if (this.core - o.core == 0) {
				return o.Type.compareTo(this.Type);
			}
			return this.core - o.core;
		}

	}

	public static String[] parsing(String data) {
		String delims = "[ ]+"; // set the space as the splitting element for parsing messages.
		String[] splitData = data.split(delims);
		return splitData;
	}

	public static void sendMSG(String msg, DataOutputStream out) {
		try {
			out.write(msg.getBytes());
			out.flush();
		} catch (IOException e) {
			System.out.println(e);
		}

	}

	public static String readMSG(BufferedReader in) throws IOException {
		String message = in.readLine();
		System.out.println("server says: " + message);
		return message;
	}

	public static void doHandShake(BufferedReader in, DataOutputStream out) {
		try {
			String received = ""; // holds received message from server

			sendMSG("HELO\n", out); // initiate handshake by sending HELO

			received = readMSG(in);
			if (received.equals("OK")) {
				sendMSG("AUTH Group34\n", out);
			} else {
				System.out.println("ERROR: OK was not received");
			}

			received = readMSG(in);
			if (received.equals("OK")) {
				sendMSG("REDY\n", out);
			} else {
				System.out.println("ERROR: OK was not received");
			}

		} catch (Exception e) {
			System.out.println(e);
		}
	}
	
	
	
	public static String[] getsAvail(String core , String memory, String disk ,BufferedReader in, DataOutputStream out) throws IOException{
		
		int fitfactor = 0;
		String[] availServer = null;
		
		sendMSG("GETS Avail "+ core +" "+ memory +" "+  disk +"\n", out);
		String rcvd = readMSG(in);
		String[] Data = parsing(rcvd); // parse DATA to find the amount of servers
		sendMSG("OK\n", out);
		
		if(Integer.parseInt(Data[1]) ==0) {
			return getsCapable(core, memory, disk, in, out);
		}
		
		int numServer = Integer.parseInt(Data[1]); // Number of servers on system.
		
		// Loop through all servers to create server list
				for (int i = 0; i < numServer; i++) {
					rcvd = readMSG(in);
					String[] stringList = parsing(rcvd);
					
					if(core == stringList[4]) {
						return stringList;
					}
					
					
					if(i == 0 || fitfactor < Integer.parseInt(core) - Integer.parseInt(stringList[4])) {
						fitfactor = Integer.parseInt(core) - Integer.parseInt(stringList[4]);
						availServer = stringList;
					}
					
				}
		
		return availServer;
		
	}
	
	
	
	public static String[] getsCapable(String core , String memory, String disk ,BufferedReader in, DataOutputStream out) throws IOException{
		
		int fitfactor = 0;
		String[] capable = null;
		
		sendMSG("GETS Capable "+ core +" "+ memory +" "+  disk +"\n", out);
		String rcvd = readMSG(in);
		String[] Data = parsing(rcvd); // parse DATA to find the amount of servers
		sendMSG("OK\n", out);
		
		// Initialise variable for server DATA
		int numServer = Integer.parseInt(Data[1]); // Number of servers on system.
		
		// Loop through all servers to create server list
		for (int i = 0; i < numServer; i++) {
			
			rcvd = readMSG(in);
			String[] stringList = parsing(rcvd);
			
			if(core == stringList[4]) { // return if perfect fit
				return stringList;
			}
			
			
			if(i == 0 || fitfactor > Integer.parseInt(core) - Integer.parseInt(stringList[4])) { //set first fit factor then check for better fit each time
				fitfactor = Integer.parseInt(core) - Integer.parseInt(stringList[4]);
				capable = stringList;
			}
			
		}
		sendMSG("OK\n", out); // catch the "." at end of data stream.
		rcvd = readMSG(in);
		return capable;
		
	}
	
	

	public static void main(String[] args) {

		try {

			Socket s = new Socket("localhost", 50000);

			BufferedReader din = new BufferedReader(new InputStreamReader(s.getInputStream()));
			DataOutputStream dout = new DataOutputStream(s.getOutputStream());

			String rcvd = "";

			// Handshake with server
			doHandShake(din, dout);

			// hold first job for later
			rcvd = readMSG(din);
			String firstjob = rcvd;

			/*
			
			// Gets command to find the largest server
			sendMSG("GETS All\n", dout); // get server DATA
			rcvd = readMSG(din);
			String[] Data = parsing(rcvd); // parse DATA to find the amount of servers
			sendMSG("OK\n", dout);
			// Initialise variable for server DATA
			int numServer = Integer.parseInt(Data[1]); // Number of servers on system.
			Server[] serverList = new Server[numServer]; // Create server array.

			// Loop through all servers to create server list
			for (int i = 0; i < numServer; i++) {
				rcvd = readMSG(din);
				String[] stringList = parsing(rcvd);
				serverList[i] = new Server(stringList[0], stringList[4]);
			}

			Arrays.sort(serverList); // Sort Servers

			// find first largest server
			String highestCore = serverList[numServer - 1].getCore();
			int highestCoreIndex = numServer - 1;

			for (int i = numServer - 1; i >= 0; i--) {
				if (serverList[i].getCore() == highestCore) {
					highestCore = serverList[i].getCore();
					highestCoreIndex = i;
				} else {
					break;
				}
			}
			System.out.println("Largest in list is " + serverList[highestCoreIndex].getType() + " with "
					+ serverList[highestCoreIndex].getCore() + " cores.");

			sendMSG("OK\n", dout); // catch the "." at end of data stream.
			rcvd = readMSG(din);

			
			*/
			
			// Schedule jobs to server
			rcvd = firstjob; // start with first job received.

			while (!rcvd.equals("NONE")) {
				String[] job = parsing(rcvd); // Get job id and job type for switch statement

				switch (job[0]) {
				case "JOBN": // Schedule job
					String [] server = getsAvail(job[4], job[5] , job[6], din, dout); // get best fit server
					sendMSG("SCHD " + job[2] + " " + server[0] + server[1] + "\n", dout);

					break;
				case "JCPL": // If job is being completed send REDY
					sendMSG("REDY\n", dout);
					break;
				case "OK": // Ask for next job
					sendMSG("REDY\n", dout);
					break;
				}
				rcvd = readMSG(din);
			}

			sendMSG("QUIT\n", dout);

			dout.close();
			s.close();

		} catch (Exception e) {
			System.out.println(e);
		}

	}

}
