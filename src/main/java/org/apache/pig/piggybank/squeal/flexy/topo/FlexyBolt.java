package org.apache.pig.piggybank.squeal.flexy.topo;

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.piggybank.squeal.flexy.executors.PipelineExecutor;
import org.apache.pig.piggybank.squeal.flexy.model.FStream;
import org.jgrapht.graph.DefaultDirectedGraph;

import storm.trident.util.ErrorEdgeFactory;
import storm.trident.util.IndexedEdge;
import storm.trident.util.TridentUtils;
import backtype.storm.generated.GlobalStreamId;
import backtype.storm.generated.Grouping;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class FlexyBolt extends BaseRichBolt {

	private static final Log log = LogFactory.getLog(FlexyBolt.class);
	private FStream root;
	int _edgeCounter = 0;
	private DefaultDirectedGraph<FStream, IndexedEdge<FStream>> G;
	private int bolt_id;
	private PipelineExecutor pipeline;
	Map<FStream, String> idMap = new HashMap<FStream, String>();
	private OutputCollector collector;
	private Integer cur_batch = null;
	private int expectedCoord = 0;
	private int seenCoord = 0;

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
		pipeline.prepare(stormConf, context, collector);
		this.collector = collector;
		
		// Determine how many coord messages to expect.
		int c = 0;
		for (Entry<GlobalStreamId, Grouping> prev : 
			context.getSources(context.getThisComponentId()).entrySet()) {
			if (prev.getKey().get_streamId().equals("coord")) {
				c++;
			}
//			log.info(prev.getKey().get_streamId() + " ---> " + context.getThisComponentId());
		}
		expectedCoord = c;
	}

	@Override
	public void execute(Tuple input) {
//		log.info(input);
		
		int batchid = input.getInteger(0);
		
//		cur_batch  = batchid;
		
		boolean send_coord = false;
		int coord_type = 1; // propagate
		
		// Determine the input type.
		if (input.getSourceStreamId().equals("commit")) {
			if (pipeline.commit(input)) {
				coord_type = 3; // commit success
			} else {
				coord_type = 4; // commit fail
			}

//			cur_batch = null;
			
			send_coord = true;
		} else if (input.getSourceStreamId().equals("coord")) {
			// Ensure the proper amount of messages came through.
			seenCoord += 1;

			// If we have received coordination messages from all our preceding nodes, start releasing.
			if (seenCoord == expectedCoord) {
				// Release the remaining tuples.
				pipeline.flush();

				// Send coord messages.
				send_coord = true;
				seenCoord = 0;
			}
		} else {
			// Either this is data or a new batch has started.
			
			// FIXME: ??? Count the message for the current batch.
			
			// FIXME: Hold one tuple for each batch until coord?
			
			// Execute the assembly.
			send_coord = pipeline.execute(input);
		}
		
		if (send_coord) {
//			log.info("Sending coord " + batchid + " " + coord_type);
			
			// Send coord messages.
			collector.emit(input, new Values(batchid, coord_type));

			// FIXME: Ack the held tuple
		}
		
		collector.ack(input);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// Add the coordination stream.
		declarer.declareStream("coord", new Fields("batchid", "type"));
		
		// Create the outputs.
		for (Entry<FStream, String> v : idMap.entrySet()) {
			declarer.declareStream(v.getValue(), 
					TridentUtils.fieldsConcat(
							new Fields("batchid"), 
							v.getKey().getOutputFields()));
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

	public void expose(FStream source) {
		if (idMap.containsKey(source)) {
			return;
		}
		String name = "b" + bolt_id + "-" + Integer.toString(idMap.size());
		idMap.put(source, name);
	}
	
	public String getStreamName(FStream source) {
		return idMap.get(source);
	}

}
