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
import java.io.PrintStream;

public class TestFlexyModel extends SquealTestBase {
    public void testParallelism() throws Exception {
    	pig.registerQuery("x = LOAD '/dev/null' USING " +
    			"org.apache.pig.piggybank.squeal.backend.storm.io.SpoutWrapper(" +
    			"'org.apache.pig.piggybank.squeal.TestSpout', '[\"windowTest\"]', '3') AS (sentence:bytearray);");
    	pig.registerQuery("count_gr = GROUP x BY RANDOM() PARALLEL 20;");

    	// Make sure FetchOnly is properly disabled.
    	ByteArrayOutputStream baos = new ByteArrayOutputStream();
    	pig.explain("x", new PrintStream(baos));
    	assertTrue(baos.toString().matches("(?si).*TestSpout-x parallel: 3.*"));
    	
    	// Ensure that parallelism doesn't bleed to the spout.
    	baos.reset();
    	pig.explain("count_gr", new PrintStream(baos));
    	
    	System.err.print(new String(baos.toByteArray()));
    	assertTrue(baos.toString().matches("(?si).*b0-TestSpout-count_gr-count_gr parallel: 3.*"));
    	assertTrue(baos.toString().matches("(?si).*b1-count_gr parallel: 20.*"));
    	
//    	explain("x");
    }
    
    public void testSpoutShuffle() throws Exception {
    	pig.registerQuery("x = LOAD '/dev/null' USING " +
    			"org.apache.pig.piggybank.squeal.backend.storm.io.SpoutWrapper(" +
    			"'org.apache.pig.piggybank.squeal.TestSpout', '[\"windowTest\"]', '3') AS (sentence:bytearray);");
    	
    	pig.getPigContext().getProperties().setProperty("x_shuffleBefore", "true");
    	pig.getPigContext().getProperties().setProperty("x_parallel", "20");
    	
    	
    	// Ensure the shuffle worked and the parallelism is correct.
    	ByteArrayOutputStream baos = new ByteArrayOutputStream();
    	pig.explain("x", new PrintStream(baos));
    	System.err.print(new String(baos.toByteArray()));
    	
    	assertTrue(baos.toString().matches("(?si).*b0-TestSpout parallel: 3.*"));
    	assertTrue(baos.toString().matches("(?si).*b1-x parallel: 20.*"));
    	
//    	explain("x");
    }
}
