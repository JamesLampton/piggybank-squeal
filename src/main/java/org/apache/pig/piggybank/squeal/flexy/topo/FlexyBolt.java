package org.apache.pig.piggybank.squeal.flexy.topo;

import java.util.ArrayDeque;
import java.util.Map;
import java.util.TreeSet;

import org.apache.pig.piggybank.squeal.flexy.executors.PipelineExecutor;
import org.apache.pig.piggybank.squeal.flexy.model.FStream;
import org.jgrapht.graph.DefaultDirectedGraph;

import storm.trident.util.ErrorEdgeFactory;
import storm.trident.util.IndexedEdge;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class FlexyBolt extends BaseRichBolt {

	private FStream root;
	int _edgeCounter = 0;
	private DefaultDirectedGraph<FStream, IndexedEdge<FStream>> G;
	private int bolt_id;
	private PipelineExecutor pipeline;

	public FlexyBolt(int bolt_id, FStream root) {
		this.bolt_id = bolt_id;
		this.root = root;
		G = new DefaultDirectedGraph<FStream, IndexedEdge<FStream>>(new ErrorEdgeFactory());
		G.addVertex(root);
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		// Create the execution pipeline.
		pipeline = new PipelineExecutor(root, G);
		pipeline.prepare(stormConf, context);
	}

	@Override
	public void execute(Tuple input) {
		// Determine the input type.
		if (input.getSourceStreamId().equals("commit")) {
			pipeline.commit(input);
		} else if (input.getSourceStreamId().equals("coord")) {
			// Ensure the proper amount of messages came through.
			// TODO
			// Release the held tuple
			
			// Release the remaining tuples.
			
			// Send coord messages.
			
		} else {
			// Determine if a new batch has started.
			
			// Count the message for the current batch.
			
			// FIXME: Hold one tuple for each batch until coord?
			
			// Execute the assembly.
			pipeline.execute(input);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// Add the coordination stream.
		declarer.declareStream("coord", new Fields("batchid"));
		
		// Create the outputs.
		for (FStream v : G.vertexSet()) {
			declarer.declareStream("b" + bolt_id + "-" + v.getName(), v.getOutputFields());
		}
	}

	public void link(FStream prev_n, FStream n) {
		// prev_n should be in the graph, n needs to be added.
		G.addVertex(n);
		
		_edgeCounter++;
		IndexedEdge<FStream> edge = new IndexedEdge<FStream>(prev_n, n, _edgeCounter);
		G.addEdge(prev_n, n, edge);
	}

	public String getName() {
		// Walk the graph in deterministic order to fetch the names.
		StringBuilder sb = new StringBuilder();
		sb.append("b");
		sb.append(bolt_id);
		
		ArrayDeque<FStream> stack = new ArrayDeque<FStream>();
		stack.add(root);
		
		while(stack.size() > 0) {
			FStream cur = stack.pollFirst();
			if (cur.getName() != null) {
				sb.append("-");
				sb.append(cur.getName());
			}
			
			// Add all the next edges to the stack.
			for (IndexedEdge<FStream> edge : new TreeSet<IndexedEdge<FStream>>(G.outgoingEdgesOf(cur))) {
				stack.addLast(edge.target);
			}
		}
		
		return sb.toString();
	}

	public int getParallelism() {
		// Find the max parallelism.
		int parallelism = 0;
		for (FStream v : G.vertexSet()) {
			if (v.getParallelism() > parallelism) {
				parallelism = v.getParallelism();
			}
		}

		return parallelism;
	}

	public FStream getRoot() {
		return root;
	}

	public String getStreamName(FStream source) {
		// Walk the graph in deterministic order to fetch the names.
		StringBuilder sb = new StringBuilder();
		sb.append("b");
		sb.append(bolt_id);
		
		sb.append("-");
		sb.append(source.hashCode());
		
		return sb.toString();
	}

}
