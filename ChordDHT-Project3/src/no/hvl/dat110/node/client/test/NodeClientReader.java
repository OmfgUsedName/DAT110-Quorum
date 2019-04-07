package no.hvl.dat110.node.client.test;

import java.io.IOException;
import java.math.BigInteger;

/**
 * exercise/demo purpose in dat110
 * @author tdoy
 *
 */

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.List;
import java.util.Random;

import no.hvl.dat110.file.FileManager;
import no.hvl.dat110.node.Message;
import no.hvl.dat110.rpc.StaticTracker;
import no.hvl.dat110.rpc.interfaces.ChordNodeInterface;
import no.hvl.dat110.util.Hash;
import no.hvl.dat110.util.Util;

public class NodeClientReader extends Thread {

	private boolean succeed = false;
	
	private String filename;
	
	public NodeClientReader(String filename) {
		this.filename = filename;
	}
	
	public void run() {
		sendRequest();
	}
	
	private void sendRequest() {
		System.out.println("This shit has executed");
		Random r = new Random();
		// Lookup(key) - Use this class as a client that is requesting for a new file and needs the identifier and IP of the node where the file is located
		// assume you have a list of nodes in the tracker class and select one randomly. We can use the Tracker class for this purpose
		String s = StaticTracker.ACTIVENODES[r.nextInt(StaticTracker.ACTIVENODES.length)];
		Registry registry;
		try {
			registry = LocateRegistry.getRegistry(StaticTracker.PORT);
		
		BigInteger node1id = Hash.hashOf(s);
		// connect to an active chord node - can use the process defined in StaticTracker 
		ChordNodeInterface p1 = (ChordNodeInterface) registry.lookup(node1id.toString());
		// Compute the hash of the node's IP address
		
		// use the hash to retrieve the ChordNodeInterface remote object from the registry
		
		// do: FileManager fm = new FileManager(ChordNodeInterface, StaticTracker.N);
		FileManager fm = new FileManager(p1, StaticTracker.N);
		// do: boolean succeed = fm.requestToReadFileFromAnyActiveNode(filename);
			succeed = fm.requestToReadFileFromAnyActiveNode(filename);
			System.out.println(succeed);
		} catch (NotBoundException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public boolean isSucceed() {
		return succeed;
	}

}
