package no.hvl.dat110.file;


/**
 * @author tdoy
 * dat110 - demo/exercise
 */

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.math.BigInteger;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.Operation;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import no.hvl.dat110.node.Message;
import no.hvl.dat110.node.OperationType;
import no.hvl.dat110.node.Operations;
import no.hvl.dat110.rpc.StaticTracker;
import no.hvl.dat110.rpc.interfaces.ChordNodeInterface;
import no.hvl.dat110.util.Hash;
import no.hvl.dat110.util.Util;

public class FileManager extends Thread {
	
	private BigInteger[] replicafiles;					// array stores replicated files for distribution to matching nodes
	private int nfiles = 4;								// let's assume each node manages nfiles (5 for now) - can be changed from the constructor
	private ChordNodeInterface chordnode;
	
	public FileManager(ChordNodeInterface chordnode, int N) throws RemoteException {
		this.nfiles = N;
		replicafiles = new BigInteger[N];
		this.chordnode = chordnode;
	}
	
	public void run() {
		
		while(true) {
			try {
				distributeReplicaFiles();
				Thread.sleep(3000);
			} catch (InterruptedException | IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	public void createReplicaFiles(String filename) {
		
		for(int i=0; i<nfiles; i++) {
			String replicafile = filename + i;
			replicafiles[i] = Hash.hashOf(replicafile);	

		}
		//System.out.println("Generated replica file keyids for "+chordnode.getNodeIP()+" => "+Arrays.asList(replicafiles));
	}
	
	public void distributeReplicaFiles() throws IOException {
		
		// lookup(keyid) operation for each replica
		// findSuccessor() function should be invoked to find the node with identifier id >= keyid and store the file (create & write the file)
		
		for(int i=0; i<replicafiles.length; i++) {
			BigInteger fileID = (BigInteger) replicafiles[i];
			ChordNodeInterface succOfFileID = chordnode.findSuccessor(fileID);
			
			// if we find the successor node of fileID, we can assign the file to the successor. This should always work even with one node
			if(succOfFileID != null) {
				succOfFileID.addToFileKey(fileID);
				String initialcontent = chordnode.getNodeIP()+"\n"+chordnode.getNodeID();
				succOfFileID.createFileInNodeLocalDirectory(initialcontent, fileID);			// copy the file to the successor local dir
			}			
		}
	}
	
	/**
	 * 
	 * @param filename
	 * @return list of active nodes in a list of messages having the replicas of this file
	 * @throws IOException 
	 */
	public Set<Message> requestActiveNodesForFile(String filename) throws IOException {
		

		// generate the N replica keyids from the filename
		this.createReplicaFiles(filename);
		// create replicas
		// findsuccessors for each file replica and save the result (fileID) for each successor 
		
		// if we find the successor node of fileID, we can retrieve the message associated with a fileID by calling the getFilesMetadata() of chordnode.
		HashSet<Message> list = new HashSet<Message>();
		for(int i=0; i<replicafiles.length; i++) {
			BigInteger fileID = (BigInteger) replicafiles[i];
			ChordNodeInterface succOfFileID = chordnode.findSuccessor(fileID);
			if(succOfFileID != null && !this.checkDuplicateActiveNode(list, succOfFileID.getFilesMetadata().get(fileID))) {
				list.add(succOfFileID.getFilesMetadata().get(fileID));
			}			
		}
		
		// save the message in a list but eliminate duplicated entries. e.g a node may be repeated because it maps more than one replicas to its id. (use checkDuplicateActiveNode)
		
		
		return list;	// return value is a Set of type Message		
	}
	
	private boolean checkDuplicateActiveNode(Set<Message> activenodesdata, Message nodetocheck) {
		
		for(Message nodedata : activenodesdata) {
			if(nodetocheck.getNodeID().compareTo(nodedata.getNodeID()) == 0)
				return true;
		}
		
		return false;
	}
	
	public boolean requestToReadFileFromAnyActiveNode(String filename) throws NotBoundException, IOException {
		
		// get all the activenodes that have the file (replicas) i.e. requestActiveNodesForFile(String filename)
		Set<Message> activenodes = this.requestActiveNodesForFile(filename);
		Message message = activenodes.iterator().next();
		// choose any available node
		// locate the registry and see if the node is still active by retrieving its remote object
		Registry registry = LocateRegistry.getRegistry(StaticTracker.PORT);
		ChordNodeInterface p1 = (ChordNodeInterface) registry.lookup(message.getNodeID().toString());
		// build the operation to be performed - Read and request for votes in existing active node message
		message.setOptype(OperationType.READ);
		Operations o1 = new Operations(p1, message, activenodes);
		//hmmmm?
		
		
		// set the active nodes holding replica files in the contact node (setActiveNodesForFile)
 		p1.setActiveNodesForFile(activenodes);
		// set the NodeIP in the message (replace ip with )
		message.setNodeIP(p1.getNodeIP());
		
		// send a request to a node and get the voters decision
		boolean decision = p1.requestReadOperation(message);
		
		
		// put the decision back in the message
		message.setAcknowledged(decision);
		// multicast voters' decision to the rest of the nodes
		p1.multicastVotersDecision(message);
		// if majority votes
		if (p1.majorityAcknowledged()) {
			p1.acquireLock();
			o1.performOperation();
			p1.multicastUpdateOrReadReleaseLockOperation(message);
			p1.releaseLocks();
		}
		// acquire lock to CS and also increments localclock
		
		// perform operation by calling Operations class
		
		// optional: retrieve content of file on local resource
		
		// send message to let replicas release read lock they are holding
		
		// release locks after operations
		
			
			
		return decision;		// change to your final answer
	}
	
	public boolean requestWriteToFileFromAnyActiveNode(String filename, String newcontent) throws NotBoundException, IOException {
		
		// get all the activenodes that have the file (replicas) i.e. requestActiveNodesForFile(String filename)
		Set<Message> activenodes = this.requestActiveNodesForFile(filename);
		Message message = activenodes.iterator().next();
		// choose any available node
		
		// locate the registry and see if the node is still active by retrieving its remote object
		Registry registry = LocateRegistry.getRegistry(StaticTracker.PORT);
		ChordNodeInterface p1 = (ChordNodeInterface) registry.lookup(message.getNodeID().toString());
		// build the operation to be performed - Read and request for votes in existing active node message
		message.setOptype(OperationType.WRITE);
		message.setNewcontent(newcontent);
		Operations o1 = new Operations(p1, message, activenodes);
		// set the active nodes holding replica files in the contact node (setActiveNodesForFile)
		p1.setActiveNodesForFile(activenodes);
		// set the NodeIP in the message (replace ip with )
		message.setNodeIP(p1.getNodeIP());
		
		
		// send a request to a node and get the voters decision
		boolean decision = p1.requestWriteOperation(message);
		// put the decision back in the message
		message.setAcknowledged(decision);
		// multicast voters' decision to the rest of the nodes
		p1.multicastVotersDecision(message);
		// if majority votes
		if (p1.majorityAcknowledged()) {
			p1.acquireLock();
			o1.performOperation();
			p1.multicastUpdateOrReadReleaseLockOperation(message);
			p1.releaseLocks();
		}
		// acquire lock to CS and also increments localclock
		
		// perform operation by calling Operations class
		
		// update replicas and let replicas release CS lock they are holding
		
		// release locks after operations
		
		return false;  // change to your final answer

	}

	/**
	 * create the localfile with the node's name and id as content of the file
	 * @param nodename
	 * @throws RemoteException 
	 */
	public void createLocalFile() throws RemoteException {
		String nodename = chordnode.getNodeIP();
		String path = new File(".").getAbsolutePath().replace(".", "");
		File fpath = new File(path+"/"+nodename);				// we'll have ../../nodename/
		if(!fpath.exists()) {
			boolean suc = fpath.mkdir();
			try {
				if(suc) {
					File file = new File(fpath+"/"+nodename); 	// end up with:  ../../nodename/nodename  (actual file no ext)
					file.createNewFile();	
					// write the node's data into this file
					writetofile(file);
				}
			} catch (IOException e) {
				
				//e.printStackTrace();
			}
		}
		
	}
	
	private void writetofile(File file) throws RemoteException {
		
		try {
			BufferedWriter bw = new BufferedWriter(new FileWriter(file, true));
			bw.write(chordnode.getNodeIP());
			bw.newLine();
			bw.write(chordnode.getNodeID().toString());
			bw.close();
									
		} catch (IOException e) {
			
			//e.printStackTrace();
		}
	}
}
