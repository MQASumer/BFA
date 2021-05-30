import java.io.*;
import java.net.*;

public class Client {

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
		return message;
	}

	public static void doHandShake(BufferedReader in, DataOutputStream out) {
		try {
			String received = ""; // holds received message from server

			sendMSG("HELO\n", out); // initiate handshake by sending HELO

			received = readMSG(in);
			if (received.equals("OK")) {
				sendMSG("AUTH aydin\n", out);
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

	public static String[] getsCapable(String core, String memory, String disk, BufferedReader in, DataOutputStream out)
			throws IOException {

		String[] prefectFit = null;
		String[] firstCapable = null;
		String[] firstCapableWJ = null; // with no more then 2 waiting jobs

		sendMSG("GETS Capable " + core + " " + memory + " " + disk + "\n", out);
		String rcvd = readMSG(in);
		String[] Data = parsing(rcvd); // parse DATA to find the amount of servers
		sendMSG("OK\n", out);

		// Initialise variable for server DATA
		int numServer = Integer.parseInt(Data[1]); // Number of servers on system.

		// Loop through all servers to create server list
		for (int i = 0; i < numServer; i++) {

			rcvd = readMSG(in);
			String[] serverData = parsing(rcvd);

			if (core == serverData[4]) { // return if perfect fit
				prefectFit = serverData;
			}

			if (i == 0) {
				firstCapable = serverData;
			}

			if (Integer.parseInt(serverData[7]) < 2 && firstCapableWJ == null) { // serverData[7] is waiting jobs
				firstCapableWJ = serverData;
			}

		}
		sendMSG("OK\n", out); // catch the "." at end of data stream.
		rcvd = readMSG(in);

		if (prefectFit != null) {
			return prefectFit;
		}

		if (firstCapableWJ != null) {
			return firstCapableWJ;
		}
		return firstCapable;

	}

	public static String[] getsAvail(String core, String memory, String disk, BufferedReader in, DataOutputStream out)
			throws IOException {

		String[] prefectFit = null;
		String[] bestfit = null;

		int fitfactor = 0;
		int prefitfactor = Integer.parseInt(core);

		sendMSG("GETS Avail " + core + " " + memory + " " + disk + "\n", out);
		String rcvd = readMSG(in);
		String[] Data = parsing(rcvd); // parse DATA to find the amount of servers
		sendMSG("OK\n", out);

		int numServer = Integer.parseInt(Data[1]); // Number of servers on system.

		if (numServer == 0) {
			rcvd = readMSG(in);// catch the "."
			return getsCapable(core, memory, disk, in, out);
		}

		// Loop through all servers to create server list
		for (int i = 0; i < numServer; i++) {
			rcvd = readMSG(in);
			String[] serverData = parsing(rcvd);

			if (i == 0) {
				fitfactor = Integer.parseInt(serverData[4]) - Integer.parseInt(core);
			}

			fitfactor = Integer.parseInt(serverData[4]) - Integer.parseInt(core);
			if (fitfactor < prefitfactor && (fitfactor <= (Integer.parseInt(core) / 2))) { // best fit for parrel jobs
				prefitfactor = Integer.parseInt(serverData[4]) - Integer.parseInt(core);
				bestfit = serverData;
			}

			if ((core == serverData[4])) { // perfect server found
				prefectFit = serverData;
			}

		}

		sendMSG("OK\n", out); // catch the "." at end of data stream.
		rcvd = readMSG(in);

		if (prefectFit != null) {
			return prefectFit;
		}

		if (bestfit != null) {
			return bestfit;
		}

		return getsCapable(core, memory, disk, in, out);

	}

	public static void main(String[] args) {

		try {

			Socket s = new Socket("localhost", 50000);

			BufferedReader din = new BufferedReader(new InputStreamReader(s.getInputStream()));
			DataOutputStream dout = new DataOutputStream(s.getOutputStream());

			// Received message from server
			String rcvd = "";

			// Handshake with server
			doHandShake(din, dout);

			// hold first job for later
			rcvd = readMSG(din);

			while (!rcvd.equals("NONE")) {
				String[] job = parsing(rcvd); // Get job id and job type for switch statement
				switch (job[0]) {
				case "JOBN": // Schedule job
					String[] server = getsAvail(job[4], job[5], job[6], din, dout); // get best Avail server
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

			sendMSG("QUIT\n", dout);

			dout.close();
			s.close();

		} catch (Exception e) {
			System.out.println(e);
		}

	}

}
