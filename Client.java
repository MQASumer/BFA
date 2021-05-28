import java.io.*;
import java.net.*;
//import java.util.*;

public class Client {

	public static String[] parsing(String data) { // used to parse thte data sent from server.
		String delims = "[ ]+"; // set space as the splitting element for parsing messages.
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
		return message;
	}

	public static void doHandShake(BufferedReader in, DataOutputStream out) {
		try {
			String received = ""; // holds received message from server

			sendMSG("HELO\n", out); // initiate handshake by sending HELO

			received = readMSG(in);
			if (received.equals("OK")) {
				sendMSG("AUTH aydin\n", out); // auth user
			} else {
				System.out.println("ERROR: OK was not received at AUTH");
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

	public static String[] getsCapable(String core, String memory, String disk, BufferedReader in, DataOutputStream out)
			throws IOException {

		String[] firstCapable = null; // Variable to hold server data

		sendMSG("GETS Capable " + core + " " + memory + " " + disk + "\n", out);
		String rcvd = readMSG(in);
		String[] Data = parsing(rcvd); // parse DATA to find the amount of servers
		sendMSG("OK\n", out);

		// Initialise variable for number servers
		int numServer = Integer.parseInt(Data[1]); // Number of servers on system.

		// Loop through all servers to create server list
		for (int i = 0; i < numServer; i++) {

			rcvd = readMSG(in);
			String[] serverData = parsing(rcvd);

			if (core == serverData[4]) { // return server if perfect fit
				return serverData;
			}

			if (i == 0) { // get first server for job.
				firstCapable = serverData;
			}

		}
		sendMSG("OK\n", out); // catch the "." at end of data stream.
		rcvd = readMSG(in);
		return firstCapable; // if no perfect fit return first capable server.

	}

	public static void main(String[] args) {

		try {

			Socket s = new Socket("localhost", 50000);

			BufferedReader din = new BufferedReader(new InputStreamReader(s.getInputStream()));
			DataOutputStream dout = new DataOutputStream(s.getOutputStream());

			String rcvd = ""; // the received message from server

			// Handshake with server
			doHandShake(din, dout);

			// Read first job
			rcvd = readMSG(din);

			while (!rcvd.equals("NONE")) {
				String[] job = parsing(rcvd); // Get job id and job type for switch statement

				switch (job[0]) {
				case "JOBN": // Schedule job
					String[] server = getsCapable(job[4], job[5], job[6], din, dout); // get perfect fit server
					sendMSG("SCHD " + job[2] + " " + server[0] + " " + server[1] + "\n", dout);
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

			sendMSG("QUIT\n", dout); // close server

			dout.close();
			s.close(); // close socket before exiting.

		} catch (Exception e) {
			System.out.println(e);
		}

	}

}
