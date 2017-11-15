/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
//			System.out.println("Fetched q@" + qName + " == " + q.hashCode());
		}
		q.add(t.toDelimitedString("\t").getBytes());
		
		if (verbose) {
			System.out.println("DEBUG: " + t);
		}
	}
}