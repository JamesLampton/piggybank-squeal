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

import java.util.Map;

import org.apache.pig.data.Tuple;
import org.junit.Test;

public class TestSquealExceptions extends SquealTestBase {

	@Test
    public void testFailureModeSpoutMapSide() throws Exception {
    	fillQueue("failureTest");
  
    	// Set the batch size to one for more fidelity.
    	String output = "/tmp/testFailures";
    	pig.registerQuery("x = LOAD '/dev/null' USING " +
    			"org.apache.pig.piggybank.squeal.backend.storm.io.SpoutWrapper(" +
    			"'org.apache.pig.piggybank.squeal.TestSpout', '[\"failureTest\", \"1\"]', '1') AS (sentence:bytearray);");
    	
    	pig.registerQuery("x = FOREACH x GENERATE FLATTEN(TOKENIZE(sentence));");
    	pig.registerQuery("x = FOREACH x GENERATE LOWER($0) AS word;");
    	
    	pig.registerQuery("fail = FOREACH x GENERATE word, org.apache.pig.piggybank.squeal.faulters.RaiseWhenEqual('little', word);");
    	
    	registerStore("fail", output, true);
    	
    	Map<Tuple, Integer> leftover = drainAndMerge("fail", null);
    	
    	// FIXME: Only emit tuples if the batch commits... -- Determine if a pipeline is a leaf.
    	// TODO: Validate stuff.
    	System.err.println("Results: " + leftover);
    	if (InMemTestQueue.getFailed("failureTest").size() != 1) {
    		fail("Unexpected number of failures: " + InMemTestQueue.getFailed("failureTest").size());
    	}
    }
	
	@Test
    public void testFailureModeReduceSide() throws Exception {
    	fillQueue("failureTest");
  
    	// Set the batch size to one for more fidelity.
    	String output = "/tmp/testFailures";
    	pig.registerQuery("x = LOAD '/dev/null' USING " +
    			"org.apache.pig.piggybank.squeal.backend.storm.io.SpoutWrapper(" +
    			"'org.apache.pig.piggybank.squeal.TestSpout', '[\"failureTest\", \"1\"]', '1') AS (sentence:bytearray);");
    	
    	pig.registerQuery("x = FOREACH x GENERATE FLATTEN(TOKENIZE(sentence));");
    	pig.registerQuery("x = FOREACH x GENERATE LOWER($0) AS word;");
    	
    	// Do an unnecessary group by to get the failure in the map on a separate node.
    	pig.registerQuery("gr = GROUP x BY RANDOM();");
    	pig.registerQuery("x = FOREACH gr GENERATE FLATTEN(x);");
    	
    	pig.registerQuery("fail = FOREACH x GENERATE word, org.apache.pig.piggybank.squeal.faulters.RaiseWhenEqual('little', word);");
    	
    	registerStore("fail", output, true);
    	
    	Map<Tuple, Integer> leftover = drainAndMerge("fail", null);
    	
    	// FIXME: Only emit tuples if the batch commits... -- Determine if a pipeline is a leaf.
    	// TODO: Validate stuff.
    	System.err.println("Results: " + leftover);
    	if (InMemTestQueue.getFailed("failureTest").size() != 1) {
    		fail("Unexpected number of failures: " + InMemTestQueue.getFailed("failureTest").size());
    	}
    }
    
//    @Test
//    public void testFailureModeCombine() throws Exception {
//
//    }
//
//    @Test
//    public void testFailureModeStore() throws Exception {
//
//    }
//    
//    @Test
//    public void testFailureModeSpout() throws Exception {
//
//    }
}
