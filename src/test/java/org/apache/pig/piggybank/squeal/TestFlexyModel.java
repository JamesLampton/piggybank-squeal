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
