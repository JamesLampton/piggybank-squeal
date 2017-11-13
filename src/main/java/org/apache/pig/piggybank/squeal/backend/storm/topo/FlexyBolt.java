/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.pig.piggybank.squeal.backend.storm.topo;

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.piggybank.squeal.backend.storm.MonkeyPatch;
import org.apache.pig.piggybank.squeal.flexy.FlexyTopology;
import org.apache.pig.piggybank.squeal.flexy.FlexyTopology.IndexedEdge;
import org.apache.pig.piggybank.squeal.flexy.components.IOutputCollector;
import org.apache.pig.piggybank.squeal.flexy.components.IRunContext;
import org.apache.pig.piggybank.squeal.flexy.components.impl.FlexyTupleFactory;
import org.apache.pig.piggybank.squeal.flexy.executors.FlexyTracer;
import org.apache.pig.piggybank.squeal.flexy.executors.PipelineExecutor;
import org.apache.pig.piggybank.squeal.flexy.model.FFields;
import org.apache.pig.piggybank.squeal.flexy.model.FStream;
import org.jgrapht.graph.DefaultDirectedGraph;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.spout.ISpoutWaitStrategy;
import backtype.storm.task.IMetricsContext;
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
	private int expectedCoord = 0;
	private int seenCoord = 0;
	private FFields input_fields;
	public static final String CRASH_ON_FAILURE_CONF = "flexy.bolt.crash.on.failure";
	boolean crashOnError = false;
	
	Map<Integer, Long> _observe_count = new HashMap<Integer, Long>();
	Map<Integer, Long> _c0_queue_acc = new HashMap<Integer, Long>();
	Map<Integer, Long> _c0_c1_diff_acc = new HashMap<Integer, Long>();
	private BoltRunContext runContext;
	private FlexyTupleFactory tFactory;

	public FlexyBolt(int bolt_id, FStream root) {
		this.bolt_id = bolt_id;
		this.root = root;
		G = new DefaultDirectedGraph<FStream, IndexedEdge<FStream>>(new FlexyTopology.ErrorEdgeFactory());
		G.addVertex(root);
	}
	
	class BoltRunContext implements IRunContext {
		
		private Map stormConf;
		private TopologyContext topoContext;
		private ISpoutWaitStrategy waitStrategy = null;

		public BoltRunContext(Map stormConf, TopologyContext topoContext) {
			this.stormConf = stormConf;
			this.topoContext = topoContext;
						
			// Pull the spout wait strategy and initialize it.
			if (stormConf.get("topology.spout.wait.strategy") != null) {
				String klassName = stormConf.get("topology.spout.wait.strategy").toString();
				try {
					Class<?> klass = ClassLoader.getSystemClassLoader().loadClass(klassName);
					waitStrategy = (ISpoutWaitStrategy) klass.newInstance();
					waitStrategy.prepare(stormConf);
				} catch (Exception e) {
					throw new RuntimeException("Unable to instantiate the wait strategy: " + klassName, e);
				}
			}
		}

		@Override
		public int getThisTaskId() {
			return topoContext.getThisTaskId();
		}

		@Override
		public String getThisComponentId() {
			return topoContext.getThisComponentId();
		}

		@Override
		public int getThisTaskIndex() {
			return topoContext.getThisTaskIndex();
		}

		@Override
		public String getStormId() {
			String ret = (String) stormConf.get("storm.id");
			if (ret == null) {
				ret = "getStormId-" + (new Random()).nextInt();
			}
			
			return ret;
		}

		@Override
		public int getPartitionIndex() {
			return topoContext.getThisTaskIndex();
		}

		@Override
		public Object get(String propertyKey) {
			return stormConf.get(propertyKey);
		}

		@Override
		public String getExposedName(FStream cur) {
			return _getExposedName(cur);
		}

		@Override
		public Map getStormConf() {
			return stormConf;
		}

		@Override
		public FFields getInputSchema() {
			return input_fields;
		}

		@Override
		public void runWaitStrategy(int emptyStreak) {
			if (this.waitStrategy != null) {
				waitStrategy.emptyEmit(emptyStreak);
			}
		}

		@Override
		public TopologyContext getStormTopologyContext() {
			return topoContext;
		}

		@Override
		public int getNumPartitions() {
			return topoContext.getComponentTasks(topoContext.getThisComponentId()).size();
		}

		@Override
		public IMetricsContext getMetricsContext() {
			return null;
		}
	}
	
	class BoltCollector implements IOutputCollector {

		private OutputCollector collector;

		public BoltCollector(OutputCollector collector) {
			this.collector = collector;
		}

		@Override
		public void emit(String exposedName, Object anchor, List values) {
			collector.emit(exposedName, (Tuple) anchor, values);
		}
		
	}
	
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		// Create the execution pipeline.
		pipeline = PipelineExecutor.build(root, G);
		
		this.runContext = new BoltRunContext(stormConf, context);
		
		pipeline.prepare(runContext, new BoltCollector(collector));
		
		this.collector = collector;
		
		// Determine how many coord messages to expect.
		expectedCoord = MonkeyPatch.getCoordCount(context);
		
		if (stormConf.containsKey(CRASH_ON_FAILURE_CONF)) {
			String v = stormConf.get(CRASH_ON_FAILURE_CONF).toString().substring(0, 1);
			crashOnError = v.equalsIgnoreCase("t") || v.equalsIgnoreCase("1");
		}
	}

	@Override
	public void execute(Tuple input) {
//		System.out.println("FlexyBolt RECEIVED:" + input);
		
		try {
			boolean send_coord = false;
			int coord_type = 1; // propagate

			// Determine the input type.
			if (input.getSourceStreamId().equals("commit")) {
				if (pipeline.commit(input.getLong(0))) {
					coord_type = 3; // commit success
				} else {
					coord_type = 4; // commit fail
				}

				//			cur_batch = null;

				send_coord = true;
			} else if (input.getSourceStreamId().equals("coord")) {
				// Ensure the proper amount of messages came through.
				seenCoord += 1;

				// Pull the coord type from the message.
				coord_type = input.getInteger(1);

				// Pull the tracer.
				FlexyTracer ft = (FlexyTracer) input.getValue(2);
				int src = input.getSourceTask();
				Long total_time = _c0_c1_diff_acc.get(src);
				if (total_time == null) {
					total_time = 0L;
					_c0_c1_diff_acc.put(src, 0L);
					_c0_queue_acc.put(src, 0L);
					_observe_count.put(src, 0L);	
				}
				
				total_time += ft.getTotalDelay();
				_c0_c1_diff_acc.put(src, total_time);
				_c0_queue_acc.put(src, _c0_queue_acc.get(src) + ft.getEmitQueueTime());
				long obs_count = _observe_count.get(src) + 1;
				_observe_count.put(src, obs_count);
				
//				log.info(getName() + " source: " + src + " avg_delay: " + (1.*total_time/obs_count));
				
				// If we have received coordination messages from all our preceding nodes, start releasing.
				if (seenCoord == expectedCoord) {
//					log.info(getName() + " seenCoord " + seenCoord + " of " + expectedCoord + " " + coord_type);
//					System.out.println(getName() + " seenCoord " + seenCoord + " of " + expectedCoord + " " + coord_type);
					// Release the remaining tuples.
					pipeline.flush(input);
					if (coord_type > 1) {
						pipeline.commit(input.getLong(0));
					}

					// Send coord messages.
					send_coord = true;
					seenCoord = 0;
				}
			} else {
//				System.out.println("FlexyBolt:" + getName() + " Got Data");
				if (tFactory == null) {
					// FIXME: Should we know this in prepare or the like?
					tFactory = FlexyTupleFactory.newFreshOutputFactory(new FFields(input.getFields().toList()));
				}
				
				// Execute the assembly.
				send_coord = pipeline.execute(tFactory.create(input.getValues()));
				if (send_coord) {
					pipeline.flush(input);
				}
			}

			if (send_coord) {			
				long batchid = input.getLong(0);
//				System.out.println("Sending coord " + batchid + " " + coord_type);

				// Send coord messages. -- Anchored.
				collector.emit("coord", input, new Values(batchid, coord_type, new FlexyTracer()));
			}

			collector.ack(input);
		} catch (Throwable e) {
			e.printStackTrace();
			collector.fail(input);
			// Throw an exception to clear any of the operator states. -- This causes issues while unit testing...
			if (crashOnError) {
				throw new RuntimeException(e);
			}
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// Add the coordination stream.
		declarer.declareStream("coord", new Fields("__batchid", "type", "tracer"));
		
		// Create the outputs.
		for (Entry<FStream, String> v : idMap.entrySet()) {
//			declarer.declareStream(v.getValue(), v.getKey().getOutputFields());
			declarer.declareStream(v.getValue(), new Fields(
					v.getKey().getOutputFields().get(0), "binnedValues"));
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

	public Integer getParallelism() {
		// Find the max parallelism.
		int parallelism = 0;
		for (FStream v : G.vertexSet()) {
			if (v.getIsStage0Agg()) {
				// Don't bleed the reduce parallelism to map.
				continue;
			}
			if (v.getParallelism() > parallelism) {
				parallelism = v.getParallelism();
			}
		}

		if (parallelism == 0) {
			return null;
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
	
	String _getExposedName(FStream source) {
		return idMap.get(source);
	}
	
	public String getStreamName(FStream source) {
		return idMap.get(source);
	}

	public void setInputSchema(FFields fFields) {
		this.input_fields = fFields;
	}
}
