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

public class TestSentenceSpout extends BaseRichSpout {
	
	public static List<byte[]> getSentList(String s) {
		ArrayList<byte[]> l = new ArrayList<byte[]>();
		l.add(s.getBytes());
		return l;
	}

	private FixedTupleSpout sp;
    
	static  List<List<byte[]>> test_tuples;
	static {
		test_tuples = new ArrayList<List<byte[]>>();
        test_tuples.add(getSentList("pepsi pepsi pepsi pepsi pepsi pepsi pepsi."));
        test_tuples.add(getSentList("The quick brown fox jumped over the lazy dog."));
        test_tuples.add(getSentList("The quick brown fox jumped over the lazy dog."));
        test_tuples.add(getSentList("The quick brown fox jumped over the lazy dog."));
        test_tuples.add(getSentList("Mary had a little lamb."));
        test_tuples.add(getSentList("This will be encoded into json."));
        test_tuples.add(getSentList("defeat of deduct went over defence before detail?"));
	}
	
    public TestSentenceSpout() {
        this(test_tuples);
	}
    
    public TestSentenceSpout(List<List<byte[]>> t) {
    	this.sp = new FixedTupleSpout(t);    	
    }

	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		sp.open(conf, context, collector);
	}

	@Override
	public void nextTuple() {
		sp.nextTuple();
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("sentence"));
	}
}
