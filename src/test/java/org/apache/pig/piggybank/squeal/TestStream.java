/*
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

import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.StorageUtil;
import org.junit.Test;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
/*
 * Testcase aimed at testing Squeal.
*/
public class TestStream extends SquealTestBase {
    
//    @Test
//    public void testUnion() throws Exception {
//    	pig.registerQuery("x = LOAD '/dev/null/0' USING " +
//    			"org.apache.pig.piggybank.squeal.backend.storm.io.SpoutWrapper(" +
//    				"'org.apache.pig.piggybank.squeal.TestSentenceSpout') AS (sentence:chararray);");
//    	pig.registerQuery("x = FOREACH x GENERATE FLATTEN(TOKENIZE(sentence));");
//    	pig.registerQuery("x = FOREACH x GENERATE LOWER($0) AS word;");
//
//    	pig.registerQuery("y = LOAD '/dev/null/1' USING " +
//    			"org.apache.pig.piggybank.squeal.backend.storm.io.SpoutWrapper(" +
//    				"'org.apache.pig.piggybank.squeal.TestSentenceSpout') AS (sentence:chararray);");
//    	
//    	pig.registerQuery("q = UNION x,y;");
//    	
//    	pig.registerQuery("y = FOREACH y GENERATE FLATTEN(TOKENIZE(sentence));");
//    	pig.registerQuery("y = FOREACH y GENERATE LOWER($0) AS word;");
//    	
//    	pig.registerQuery("z = UNION x,y;");
//    	pig.registerQuery("r = UNION q,z;");
//
//    	// FIXME: Fails.
////    	pig.registerQuery("STORE r INTO 'fake/pathr';");
////    	explain("r");
//    }
//    
//    @Test
//    public void testJoin() throws Exception {
//    	String output = "/tmp/testJoin";
//    	
//    	// Create the input file ourselves.
//    	File stopfile = new File(STOPWORDS_FILE);
////    	System.out.println("f:" + stopfile.getAbsolutePath());
//    	FileWriter fh = new FileWriter(stopfile);
//    	for (String w : STOPWORDS) {
////    		System.out.println("w:" + w);
//    		fh.write(w);
//    		fh.write("\n");
//    	}
//    	fh.close();
//    	
//    	pig.registerQuery("x = LOAD '/dev/null/0' USING " +
//    			"org.apache.pig.piggybank.squeal.backend.storm.io.SpoutWrapper(" +
//    				"'org.apache.pig.piggybank.squeal.TestSentenceSpout') AS (sentence:chararray);");
//    	pig.registerQuery("x = FOREACH x GENERATE FLATTEN(TOKENIZE(sentence));");
//    	pig.registerQuery("x = FOREACH x GENERATE LOWER($0) AS word, 'x';");
//
//    	pig.registerQuery("y = LOAD '/dev/null/1' USING " +
//    			"org.apache.pig.piggybank.squeal.backend.storm.io.SpoutWrapper(" +
//    				"'org.apache.pig.piggybank.squeal.TestSentenceSpout2') AS (sentence:chararray);");
//    	pig.registerQuery("y = FOREACH y GENERATE FLATTEN(TOKENIZE(sentence));");
//    	pig.registerQuery("y = FOREACH y GENERATE LOWER($0) AS word, 'y';");
//
//    	pig.registerQuery("stoplist = LOAD '" + STOPWORDS_FILE + "' AS (stopword:chararray);");
//    	
//    	pig.registerQuery("wordsr = JOIN x BY word, stoplist BY stopword USING 'replicated';");
////    	explain("wordsr");
//    	registerStore("wordsr", output);
//
//    	// Tests for mixed static/dynamic stuff.
//    	pig.registerQuery("stoplist2 = LOAD '" + STOPWORDS_FILE + "' AS (stopword2:chararray);");
//    	pig.registerQuery("stoplist3 = JOIN stoplist2 BY stopword2, stoplist BY stopword USING 'replicated';");
//    	pig.registerQuery("stoplist3 = FOREACH stoplist3 GENERATE stopword2, stopword AS cheese;");
//    	
//    	pig.registerQuery("x = FILTER x BY $0 == 'the';");
////    	pig.registerQuery("words_sl3 = JOIN x BY word, stoplist BY stopword USING 'replicated';");
//    	pig.registerQuery("words_sl3 = JOIN x BY word, stoplist3 BY stopword2;");
//    	pig.registerQuery("words_sl3_fe = FOREACH words_sl3 GENERATE word;");
//
////    	pig.registerQuery("words_sl3 = FOREACH words_sl3 GENERATE word;");
////    	pig.registerQuery("words_sl3_2 = JOIN words_sl3 BY word, stoplist3 BY stopword;");
//    	
////    	explain("words_simple_join");
////    	props.setProperty("words_sl3_store_opts", "{\"StateFactory\":\"edu.umd.estuary.storm.trident.state.RedisState\", \"StaticMethod\": \"fromJSONArgs\", \"args\": [{\"servers\": \"localhost\", \"dbNum\": 3, \"expiration\": 300, \"serializer\":\"org.apache.pig.backend.storm.state.GZPigSerializer\", \"key_serializer\":\"org.apache.pig.backend.storm.state.PigTextSerializer\"}]}");
//
////    	String redis_store_opts = "{\"StateFactory\":\"edu.umd.estuary.storm.trident.state.RedisState\", \"StaticMethod\": \"fromJSONArgs\", \"args\": [{\"servers\": \"localhost\", \"dbNum\": 3, \"expiration\": 300, \"serializer\":\"org.apache.pig.backend.storm.state.GZPigSerializer\", \"key_serializer\":\"org.apache.pig.backend.storm.state.PigTextSerializer\"}]}";
////    	props.setProperty("words_sl3_store_opts", "{\"StateFactory\":\"org.apache.pig.backend.storm.state.MultiState\", \"StaticMethod\": \"fromJSONArgs\", \"args\": [{\"default\": {}, "+
////    			"\"1\": " + redis_store_opts + " }]}");
//    	
////    	props.setProperty("words_sl3_store_opts", "{\"StateFactory\":\"edu.umd.estuary.storm.trident.state.RedisState\", \"StaticMethod\": \"fromJSONArgs\", \"args\": [{\"servers\": \"localhost\", \"dbNum\": 3, \"serializer\":\"org.apache.pig.backend.storm.state.GZPigSerializer\", \"key_serializer\":\"org.apache.pig.backend.storm.state.PigTextSerializer\"}]}");
////    	props.setProperty("words_sl3_window_opts", "{\"0\":2}");
////    	explain("words_sl3_fe");
////    	registerStore("words_sl3", output);
//
//    	
////    	pig.registerQuery("x = FILTER x BY $0 == 'the';");
////    	pig.registerQuery("y = FILTER y BY $0 == 'the';");
//    	pig.registerQuery("silly = JOIN x BY word, y BY word;");
////    	registerStore("x", output);
////    	explain("silly");    	
////    	registerStore("silly", output);
//
//    	
//    	// Test Parallelism for static jobs. FIXME: I think this requires minicluster.
////    	pig.registerQuery("words_par = JOIN x BY word, stoplist2 BY stopword2 PARALLEL 200;");
////    	explain("words_par");
//    	
//    	stopfile.delete();
//    }
        
    @Test
    public void testWCHist () throws Exception {
//    	props.setProperty("pig.exec.nocombiner", "true");
    	String output = "/tmp/testWCHist";
    	
    	fillQueue("wcTest");
    	
    	pig.registerQuery("x = LOAD '/dev/null' USING " +
    			"org.apache.pig.piggybank.squeal.backend.storm.io.SpoutWrapper(" +
    				"'org.apache.pig.piggybank.squeal.TestSpout', '[\"wcTest\"]', '3') AS (sentence:bytearray);");

    	// STREAM is asynchronous is how it returns results, we don't have enough to make it work in this case.
//    	pig.registerQuery("x = STREAM x THROUGH `tr -d '[:punct:]'` AS (sentence:chararray);");

    	pig.registerQuery("x = FOREACH x GENERATE FLATTEN(TOKENIZE(sentence));");
    	pig.registerQuery("x = FOREACH x GENERATE LOWER($0) AS word;");

//    	props.setProperty("count_gr_store_opts", "{\"StateFactory\":\"edu.umd.estuary.storm.trident.state.RedisState\", \"StaticMethod\": \"fromJSONArgs\", \"args\": [{\"servers\": \"localhost\", \"dbNum\": 1, \"expiration\": 300, \"serializer\":\"org.apache.pig.backend.storm.state.PigSerializer\", \"key_serializer\":\"org.apache.pig.backend.storm.state.PigTextSerializer\"}]}");
    	pig.registerQuery("count_gr = GROUP x BY word;");
    	pig.registerQuery("count = FOREACH count_gr GENERATE group AS word, COUNT(x) AS wc;");
    	
//    	props.setProperty("hist_gr_store_opts", "{\"StateFactory\":\"edu.umd.estuary.storm.trident.state.RedisState\", \"StaticMethod\": \"fromJSONArgs\", \"args\": [{\"servers\": \"localhost\", \"dbNum\": 2, \"expiration\": 300, \"serializer\":\"org.apache.pig.backend.storm.state.PigSerializer\", \"key_serializer\":\"org.apache.pig.backend.storm.state.PigTextSerializer\"}]}");
    	pig.registerQuery("hist_gr = GROUP count BY wc;");
    	pig.registerQuery("hist = FOREACH hist_gr GENERATE group AS wc, COUNT(count) AS freq;");
    	pig.registerQuery("hist = FILTER hist BY freq > 0;");
    	
    	/*
DEBUG: (3,6,1)
DEBUG: (4,1,1)
DEBUG: (1,19,1)
DEBUG: (6,2,1)
    	 */
//    	explain("hist");
    	registerStore("hist", output, true);
    	
    	List<String> expected = new ArrayList<String>();
    	expected.add("3\t6");
    	expected.add("4\t1");
    	expected.add("1\t19");
    	expected.add("6\t2");
    	
    	Map<Tuple, Integer> leftover = drainAndMerge("hist", expected);
    	if (leftover.size() != 0) {
    		fail("Unexpected return value: " + leftover);
    	}
    	if (InMemTestQueue.getFailed("hist").size() > 0) {
    		fail("Some messages failed");
    	}
    }

    public void testWindow() throws Exception {
    	fillQueue("windowTest");
    	
    	String output = "/tmp/testWindow";
    	pig.registerQuery("x = LOAD '/dev/null' USING " +
    			"org.apache.pig.piggybank.squeal.backend.storm.io.SpoutWrapper(" +
    			"'org.apache.pig.piggybank.squeal.TestSpout', '[\"windowTest\"]', '3') AS (sentence:bytearray);");
    	
    	pig.registerQuery("x = FOREACH x GENERATE FLATTEN(TOKENIZE(sentence));");
    	pig.registerQuery("x = FOREACH x GENERATE LOWER($0) AS word;");
    	pig.registerQuery("x = FILTER x BY word == 'the';");
    	props.setProperty("count_gr_window_opts", "{\"0\":2}");
//    	props.setProperty("count_gr_store_opts", "{\"StateFactory\":\"edu.umd.estuary.storm.trident.state.RedisState\", \"StaticMethod\": \"fromJSONArgs\", \"args\": [{\"servers\": \"localhost\", \"dbNum\": 0, \"expiration\": 300, \"serializer\":\"org.apache.pig.backend.storm.state.PigSerializer\", \"key_serializer\":\"org.apache.pig.backend.storm.state.PigTextSerializer\"}]}");
    	pig.registerQuery("count_gr = GROUP x BY word;");
    	pig.registerQuery("count = FOREACH count_gr GENERATE group AS word, COUNT(x) AS wc;");
//    	pig.registerQuery("count = FILTER count by wc > 0"); // FIXME: BUGGG!!!! necessary due to issue with combiner handling oddity.

    	registerStore("count", output, true);
//    	explain("count");
    	
    	List<String> expected = new ArrayList<String>();
    	expected.add("the\t2");
    	expected.add("the\t2");
    	expected.add("the\t2");
    	
    	Map<Tuple, Integer> leftover = drainAndMerge("count", expected);
    	
    	// Remove the funk from the combiner issue
    	byte[] buf = "\t0".getBytes();
    	leftover.remove(StorageUtil.bytesToTuple(buf, 0, buf.length, (byte) '\t'));
    	
    	if (leftover.size() != 0) {
    		fail("Unexpected return value: " + leftover);
    	}
    	
    	if (InMemTestQueue.getFailed("hist").size() > 0) {
    		fail("Some messages failed");
    	}
    }
    
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
    	
    	assertTrue(baos.toString().matches("(?si).*b0-TestSpout-count_gr-count_gr parallel: 3.*"));
    	assertTrue(baos.toString().matches("(?si).*b1 parallel: 20.*"));
    	
    	System.err.print(new String(baos.toByteArray()));
//    	explain("x");
    }
   
}
