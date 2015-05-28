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
import java.util.List;
import java.util.Map;

import org.apache.pig.piggybank.squeal.flexy.model.FStream;
import org.jgrapht.graph.DefaultDirectedGraph;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;
import storm.trident.tuple.TridentTupleView;
import storm.trident.tuple.TridentTupleView.OperationOutputFactory;
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
		private TridentTuple.Factory output_tf;
		private Map stormConf;
		private TopologyContext context;

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

			// Create an output tuple factory.
			
			// Prepare myself.
//			cur.prepare(stormConf, context, triContext);

			if (parent_tf == null) {
				// FIXME -- get the input fields... -- create a root.  This can happen in a shuffle.
			}
			
			TridentOperationContext triContext = new TridentOperationContext(context, parent_tf);
			
			switch (cur.getType()) {
			case FUNCTION:
				cur.getFunc().prepare(stormConf, triContext);				
				break;
			case GROUPBY:
				output_tf = new TridentTupleView.OperationOutputFactory(parent_tf, cur.getOutputFields());
				// TODO -- prepare the agg stuff.
				break;
			case PROJECTION:
				output_tf = triContext.makeProjectionFactory(cur.getOutputFields());
				break;
			case SPOUT:
				output_tf = new TridentTupleView.RootFactory(cur.getOutputFields());
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
			// TODO Auto-generated method stub			
		}

		public boolean execute(Tuple input) {
			boolean ret = false;
			
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
				ret = true;
				break;
			default:
				throw new RuntimeException("Unknown node type:" + cur.getType());
			}
			
			// TODO Auto-generated method stub
			return ret;
		}

		@Override
		public void emit(List<Object> values) {
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
