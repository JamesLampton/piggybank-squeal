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

package org.apache.pig.piggybank.squeal.flexy.executors;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.piggybank.squeal.backend.storm.io.ImprovedRichSpoutBatchExecutor.CaptureCollector;
import org.apache.pig.piggybank.squeal.flexy.model.FStream;
import org.jgrapht.graph.DefaultDirectedGraph;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;
import storm.trident.tuple.TridentTupleView;
import storm.trident.tuple.TridentTupleView.FreshOutputFactory;
import storm.trident.tuple.TridentTupleView.OperationOutputFactory;
import storm.trident.tuple.TridentTupleView.ProjectionFactory;
import storm.trident.tuple.TridentTupleView.RootFactory;
import storm.trident.util.IndexedEdge;

public class PipelineExecutor {

	private FStream root;
	private DefaultDirectedGraph<FStream, IndexedEdge<FStream>> subG;
	private Executor exec;
	private OutputCollector collector;

	public PipelineExecutor(FStream root,
			DefaultDirectedGraph<FStream, IndexedEdge<FStream>> g) {
		this.root = root;
		this.subG = g;
	}

	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		
		// Build up the pipeline.	
		exec = build(root);
		exec.prepare(stormConf, context, collector);
	}
	
	static class Executor implements TridentCollector {
		private FStream cur;
		private List<Executor> children;
		private OutputCollector collector;
//		private TridentTuple.Factory output_tf;
		private Map stormConf;
		private TopologyContext context;
		private static final Log log = LogFactory.getLog(Executor.class);
		
		// Spout stuff
		CaptureCollector _collector = new CaptureCollector();
		private int maxBatchSize;
		public static final String MAX_BATCH_SIZE_CONF = "topology.spout.max.batch.size";
		static public final int DEFAULT_MAX_BATCH_SIZE = 1000;
		
		// Assume single active batch at this time.
		private Map<Long, List<Object>> idsMap = new HashMap<Long, List<Object>>();
		private OperationOutputFactory op_output_tf;
		private ProjectionFactory proj_output_tf;
		private FreshOutputFactory root_output_tf;
		private TridentTuple parent;

		public Executor(FStream cur, List<Executor> children) {
			this.cur = cur;
			this.children = children;
		}

		public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
			prepare(stormConf, context, collector, null);
		}
		
		private void prepare(Map stormConf, TopologyContext context, OutputCollector collector, TridentTuple.Factory parent_tf) {			

			this.stormConf = stormConf;
			this.context = context;
			
			// Stash this for use later.
			this.collector = collector;
			
			if (parent_tf == null) {
				// FIXME -- get the input fields... -- create a root.  This can happen in a shuffle. ??
				System.err.println("Cur: " + cur + " null parent.");
			}
			
			TridentOperationContext triContext = new TridentOperationContext(context, parent_tf);

			TridentTuple.Factory output_tf;
			// Create an output tuple factory.
			switch (cur.getType()) {
			case FUNCTION:
				cur.getFunc().prepare(stormConf, triContext);
				// Create a projection for the input.
				proj_output_tf = triContext.makeProjectionFactory(cur.getInputFields());
				op_output_tf = new TridentTupleView.OperationOutputFactory(parent_tf, cur.getAppendOutputFields());
				output_tf = op_output_tf;
				break;
			case GROUPBY:
				op_output_tf = new TridentTupleView.OperationOutputFactory(parent_tf, cur.getAppendOutputFields());
				output_tf = op_output_tf;
				// TODO -- prepare the agg stuff.
				break;
			case PROJECTION:
				proj_output_tf = triContext.makeProjectionFactory(cur.getAppendOutputFields());
				output_tf = proj_output_tf;
				break;
			case SPOUT:
				Number batchSize = (Number) stormConf.get(MAX_BATCH_SIZE_CONF);
		        if(batchSize==null) batchSize = DEFAULT_MAX_BATCH_SIZE;
		        maxBatchSize = batchSize.intValue();
		        
				// Prepare the spout
				cur.getSpout().open(stormConf, context, new SpoutOutputCollector(_collector));
				
				root_output_tf = new TridentTupleView.FreshOutputFactory(cur.getAppendOutputFields());
				output_tf = root_output_tf;
				break;
			default:
				throw new RuntimeException("Unknown node type:" + cur.getType());
			}
			
			for (Executor child : children) {
				child.prepare(stormConf, context, collector, output_tf);
			}
		}

		public boolean commit(Tuple input) {
			boolean ret = true;
			
			switch (cur.getType()) {
			case FUNCTION:
				// TODO
				break;
			case GROUPBY:
				// TODO
				break;
			case PROJECTION:
				// TODO
				break;
			case SPOUT:
				// TODO
				break;
			default:
				throw new RuntimeException("Unknown node type:" + cur.getType());
			}
			
			return ret;
		}

		public void flush() {
			switch (cur.getType()) {
			case FUNCTION:
				// TODO
				break;
			case GROUPBY:
				// TODO
				break;
			case PROJECTION:
				// TODO
				break;
			case SPOUT:
				// TODO
				break;
			default:
				throw new RuntimeException("Unknown node type:" + cur.getType());
			}	
		}

		private void execute(TridentTuple tup) {
			log.info("execute: " + cur + " " + tup);
			parent = tup;

			switch (cur.getType()) {
			case FUNCTION:
				// Project as appropriate
				tup = proj_output_tf.create(tup);
				cur.getFunc().execute(tup, this);
				break;
			case GROUPBY:
				// TODO
				break;
			case PROJECTION:
				emit(null);
				break;
			case SPOUT:
				throw new RuntimeException("Spouts shouldn't be called in this manner...");
			default:
				throw new RuntimeException("Unknown node type:" + cur.getType());
			}
		}
		
		public boolean execute(Tuple input) {
			boolean ret = false;
			
			switch (cur.getType()) {
			case FUNCTION:
			case GROUPBY:
			case PROJECTION:
				// Create the appropriate tuple and move along.
				// TODO
				break;
			case SPOUT:
				// Check on failures
				long txid = input.getLong(0);
				// TODO
//				if(idsMap.containsKey(txid)) {
//	                fail(txid);
//	            }
				
				// Release some tuples.
				_collector.reset(this);
				for(int i=0; i < maxBatchSize; i++) {
	                cur.getSpout().nextTuple();
	                if(_collector.numEmitted < i) {
	                    break;
	                }
	            }
				
				// Save off the emitted ids.
				idsMap.put(txid, _collector.ids);
				
				ret = true;
				break;
			default:
				throw new RuntimeException("Unknown node type:" + cur.getType());
			}
			
			return ret;
		}

		@Override
		public void emit(List<Object> values) {
			log.info("Emit: " + values);
			
			TridentTuple tup = null;
			// Use the appropriate output factory to create the next tuple.
			switch (cur.getType()) {
			case FUNCTION:
				tup = op_output_tf.create((TridentTupleView) parent, values);
				break;
			case GROUPBY:
				// TODO
				break;
			case PROJECTION:
				tup = proj_output_tf.create(parent);
				break;
			case SPOUT:
				tup = root_output_tf.create(values);
				break;
			default:
				throw new RuntimeException("Unknown node type:" + cur.getType());
			}
			
			// Call all the children.
			for (Executor child : children) {
				child.execute(tup);
			}
			
			// Emit if necessary.
			
			// TODO Auto-generated method stub
			
		}

		@Override
		public void reportError(Throwable t) {
			// TODO Auto-generated method stub
			
		}
	}

	private Executor build(FStream cur) {
		ArrayList<Executor> children = new ArrayList<Executor>();
		for (IndexedEdge<FStream> edge : subG.outgoingEdgesOf(cur)) {
			children.add(build(edge.target));
		}
		return new Executor(cur, children);
	}
	
	public boolean execute(Tuple input) {
		// Execute the pipeline.
		return exec.execute(input);
	}

	public boolean commit(Tuple input) {
		return exec.commit(input);
	}

	public void flush() {
		exec.flush();
	}
}
