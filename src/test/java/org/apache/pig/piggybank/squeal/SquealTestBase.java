/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.pig.piggybank.squeal;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.util.StorageUtil;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import junit.framework.TestCase;

public abstract class SquealTestBase extends TestCase {
	protected final Log log = LogFactory.getLog(getClass());
	protected static final String STOPWORDS_FILE = "stop_words.txt";
	protected static final String[] STOPWORDS = {
			"a", "able", "about", "across", "after", "all", "almost", "also", 
			"am", "among", "an", "and", "any", "are", "as", "at", "be", "because", 
			"been", "but", "by", "can", "cannot", "could", "dear", "did", "do", 
			"does", "either", "else", "ever", "every", "for", "from", "get", 
			"got", "had", "has", "have", "he", "her", "hers", "him", "his", 
			"how", "however", "i", "if", "in", "into", "is", "it", "its", 
			"just", "least", "let", "like", "likely", "may", "me", "might", 
			"most", "must", "my", "neither", "no", "nor", "not", "of", "off", 
			"often", "on", "only", "or", "other", "our", "own", "rather", "said", 
			"say", "says", "she", "should", "since", "so", "some", "than", "that", 
			"the", "their", "them", "then", "there", "these", "they", "this", "tis", 
			"to", "too", "twas", "us", "wants", "was", "we", "were", "what", "when", 
			"where", "which", "while", "who", "whom", "why", "will", "with", "would", 
			"yet", "you", "your", "que", "lol", "dont"};

	protected PigServer pig;
	protected String test_tuples;
	protected Properties props;
	protected static boolean runMiniCluster = false;

	public void fillQueue(String qName) {
		BlockingQueue<byte[]> q = InMemTestQueue.getQueue(qName);
		q.add("pepsi pepsi pepsi pepsi pepsi pepsi pepsi.".getBytes());
		q.add("The quick brown fox jumped over the lazy dog.".getBytes());
		q.add("The quick brown fox jumped over the lazy dog.".getBytes());
		q.add("The quick brown fox jumped over the lazy dog.".getBytes());
		q.add("Mary had a little lamb.".getBytes());
		q.add("This will be encoded into json.".getBytes());
		q.add("defeat of deduct went over defence before detail?".getBytes());
		InMemTestQueue.getFailed(qName).clear();
	}

	public Map<Tuple, Integer> drainAndMerge(String qName, List<String> validate) throws ExecException {
		BlockingQueue<byte[]> q = InMemTestQueue.getQueue(qName);
		List<byte[]> results = new ArrayList<byte[]>();
		q.drainTo(results);

		System.out.println("Fetched q@" + qName + " == " + q.hashCode() + " result count: " + results.size());

		// Parse the results into tuples, then run the merge.
		Map<Tuple, Integer> mt = new HashMap<Tuple, Integer>();
		for (byte[] buf : results) {
			// Parse the tuple.
			//	    		System.out.println("ZZ " + new String(buf));
			Tuple t = StorageUtil.bytesToTuple(buf, 0, buf.length, (byte) '\t');
			// Get ready to "copy".
			List<Object> contents = t.getAll();
			// Pull the sign.
			Integer sign = DataType.toInteger(contents.remove(t.size() - 1));
			// Create a new tuple.
			t = TupleFactory.getInstance().newTuple(contents);
			// Pull the current value
			Integer cur = mt.get(t);
			if (cur == null) {
				mt.put(t, sign);
			} else {
				cur += sign;
				if (cur == 0) {
					mt.remove(t);
				} else {
					mt.put(t, cur);
				}
			}	
		}

		if (validate != null) {
			// Parse the validation set and check to see if all the tuples are covered.
			for (String s : validate) {
				byte[] buf = s.getBytes();
				Tuple t = StorageUtil.bytesToTuple(buf, 0, buf.length, (byte) '\t');
				int sign = -1;
				// Pull the current value
				Integer cur = mt.get(t);
				if (cur == null) {
					mt.put(t, sign);
				} else {
					cur += sign;
					if (cur == 0) {
						mt.remove(t);
					} else {
						mt.put(t, cur);
					}
				}
			}
		} else {
			System.out.println(mt);
		}

		return mt;
	}

	@Override
	@Before
	public void setUp() throws Exception {
		System.setProperty("hadoop.log.dir", "build/test/logs");

		if (runMiniCluster) {
			//FIXME    		cluster = MiniCluster.buildCluster();
			// Write out a stop list.    	
			//FIXME Util.createInputFile(cluster, STOPWORDS_FILE, STOPWORDS);
			//	    		pig = new PigServer(ExecType.STORM, cluster.getProperties());
			//FIXME   	pig = new PigServer(new StormExecType(), cluster.getProperties());
		} else {
			pig = new PigServer("storm-local");
			//	        	pig = new PigServer("storm-local");
			//	        	pig = new PigServer("local");
		}

		props = pig.getPigContext().getProperties();    	
		props.setProperty("pig.streaming.run.test.cluster", "true");
		props.setProperty("pig.streaming.run.test.cluster.direct", "true");
		props.setProperty("pig.streaming.run.test.cluster.wait_time", "25000");
		props.setProperty("pig.streaming.debug", "true");
		
	}

	@AfterClass
	public static void oneTimeTearDown() throws Exception {
		if (runMiniCluster) {
			//FIXME cluster.shutDown();
		}
	}

	@After
	public void tearDown() throws Exception {
		if (runMiniCluster) {
			//FIXME Util.deleteFile(cluster, STOPWORDS_FILE);
		}
	}

	public void explain(String alias) throws IOException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		pig.explain(alias, new PrintStream(baos));  	
		System.err.print(new String(baos.toByteArray()));    	
	}
	
    protected void registerStore(String alias, String path, boolean inMem) throws Exception {
    	pig.deleteFile(path);
    	if (inMem) {
    		// Pull the queue and clear it.
    		InMemTestQueue.getQueue(alias).clear();
    		InMemTestQueue.getFailed(alias).clear();
    		
    		pig.registerQuery("STORE " + alias + " INTO '" + path + "' USING org.apache.pig.piggybank.squeal.backend.storm.io.SignStoreWrapper('org.apache.pig.piggybank.squeal.TestStore', '"+ alias + "', 'true');");
    	} else {
        	pig.registerQuery("STORE " + alias + " INTO '" + path + "' USING org.apache.pig.piggybank.squeal.backend.storm.io.SignStoreWrapper('org.apache.pig.piggybank.squeal.backend.storm.io.DebugOutput');");    		
    	}
    }
}
