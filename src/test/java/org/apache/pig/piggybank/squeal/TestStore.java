package org.apache.pig.piggybank.squeal;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;

import org.apache.pig.builtin.PigStorage;
import org.apache.pig.data.Tuple;

public class TestStore extends PigStorage {
	private String qName;
	private boolean verbose = false;
	private BlockingQueue<byte[]> q;

	public TestStore(String qName, String verbose) {
		this.qName = qName;
		this.verbose  = verbose.equalsIgnoreCase("true");
	}
	
	public TestStore(String qName) {
		this(qName, "true");
	}
	
	@Override
	public void putNext(Tuple t) throws IOException {
		if (q == null) {
			q = InMemTestQueue.getQueue(qName);
//			System.err.println("Fetched q@" + qName + " == " + q.hashCode());
		}
		q.add(t.toDelimitedString("\t").getBytes());
		
		if (verbose) {
			System.out.println("DEBUG: " + t);
		}
	}
}