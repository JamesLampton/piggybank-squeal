package org.apache.pig.piggybank.squeal;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class TestSpout extends BaseRichSpout {
	private String qName;
	private BlockingQueue<byte[]> q;
	private SpoutOutputCollector collector;

	public TestSpout(String qName) {
		this.qName = qName;
	}
	
	@Override
	public void open(Map conf, TopologyContext context,	SpoutOutputCollector collector) {
		q = InMemTestQueue.getQueue(qName);
		this.collector = collector;
	}

	@Override
	public void nextTuple() {
		try {
			byte[] cur = q.poll(10, TimeUnit.MILLISECONDS);
			if (cur != null) {
				collector.emit(new Values(cur));
			}
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("buf"));
	}
}