package org.apache.pig.test.storm;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.testing.FixedTupleSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;

public class TestSentenceSpout2 extends TestSentenceSpout {
	
	static  List<List<byte[]>> test_tuples2;
	static {
		test_tuples2 = new ArrayList<List<byte[]>>();
        test_tuples2.add(getSentList("hello world"));
        test_tuples2.add(getSentList("more cowbell"));
        test_tuples2.add(getSentList("are these even sentences?"));
        test_tuples2.add(getSentList("it was the best of times it was the worst of times"));
        test_tuples2.add(getSentList("a rose is a rose is a rose"));
        test_tuples2.add(getSentList("my mother is a fish"));
	}
	
    public TestSentenceSpout2() {
        super(test_tuples2);
	}
}
