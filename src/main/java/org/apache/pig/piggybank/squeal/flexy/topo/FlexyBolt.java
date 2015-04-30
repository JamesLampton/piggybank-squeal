package org.apache.pig.piggybank.squeal.flexy.topo;

import java.util.Map;

import org.apache.pig.piggybank.squeal.flexy.model.FStream;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class FlexyBolt extends BaseRichBolt {

	public FlexyBolt(FStream root) {
		// TODO
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		
	}

	public void link(FStream prev_n, FStream n) {
		// TODO Auto-generated method stub
		
	}

}
