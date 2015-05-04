package org.apache.pig.piggybank.squeal.flexy.executors;

import java.util.Map;

import org.apache.pig.piggybank.squeal.flexy.model.FStream;
import org.jgrapht.graph.DefaultDirectedGraph;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import storm.trident.util.IndexedEdge;

public class PipelineExecutor {

	public PipelineExecutor(FStream root,
			DefaultDirectedGraph<FStream, IndexedEdge<FStream>> g) {
		// TODO Auto-generated constructor stub
	}

	public void prepare(Map stormConf, TopologyContext context) {
		// TODO Auto-generated method stub		
	}

	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		
	}

	public void commit(Tuple input) {
		// TODO Auto-generated method stub
		
	}

}
