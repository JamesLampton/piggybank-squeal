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
import org.apache.hadoop.io.Writable;
import org.apache.pig.piggybank.squeal.backend.storm.io.ImprovedRichSpoutBatchExecutor.CaptureCollector;
import org.apache.pig.piggybank.squeal.flexy.model.FStream;
import org.apache.pig.piggybank.squeal.flexy.model.FStream.NodeType;
import org.apache.pig.piggybank.squeal.flexy.topo.FlexyBolt;
import org.jgrapht.graph.DefaultDirectedGraph;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;
import storm.trident.tuple.TridentTuple.Factory;
import storm.trident.tuple.TridentTupleView;
import storm.trident.tuple.TridentTupleView.FreshOutputFactory;
import storm.trident.tuple.TridentTupleView.OperationOutputFactory;
import storm.trident.tuple.TridentTupleView.ProjectionFactory;
import storm.trident.tuple.TridentTupleView.RootFactory;
import storm.trident.util.IndexedEdge;

public class PipelineExecutor implements TridentCollector {
	private FStream cur;
	private List<PipelineExecutor> children;
	private OutputCollector collector;
//	private TridentTuple.Factory output_tf;
	private Map stormConf;
	private TopologyContext context;
	private static final Log log = LogFactory.getLog(PipelineExecutor.class);
	
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
	private String exposedName;
	private Stage0Executor stage0Exec;
	private Stage1Executor stage1Exec;
	private FreshOutputFactory parent_root_tf;

	private PipelineExecutor(FStream cur, List<PipelineExecutor> children) {
		this.cur = cur;
		this.children = children;
	}

	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector,
			FlexyBolt flexyBolt) {
		prepare(stormConf, context, collector, null, flexyBolt);
	}
	
	private void prepare(Map stormConf, TopologyContext context, 
			OutputCollector collector, TridentTuple.Factory parent_tf,
			FlexyBolt flexyBolt) {			

		this.stormConf = stormConf;
		this.context = context;
		
		exposedName = flexyBolt.getExposedName(cur);
		
		// Stash this for use later.
		this.collector = collector;
		
		if (parent_tf == null && cur.getType() != NodeType.SPOUT) {
			log.info("NULL tf: " + cur + " " + flexyBolt.getInputSchema());
			parent_tf = parent_root_tf = new TridentTupleView.FreshOutputFactory(flexyBolt.getInputSchema());
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
			proj_output_tf = triContext.makeProjectionFactory(cur.getInputFields());
			root_output_tf = new TridentTupleView.FreshOutputFactory(cur.getOutputFields());
			output_tf = root_output_tf;
			
			// Prepare the agg stuff.
			if (cur.getIsStage0Agg()) {
				this.stage0Exec = new Stage0Executor(cur.getStage0Agg());
				stage0Exec.prepare(stormConf, context, this);
			} else {
				this.stage1Exec = new Stage1Executor(cur.getStage1Agg(), cur.getStorageAgg(), cur.getStateFactory());
				stage1Exec.prepare(stormConf, context, this);
			}
			
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
		
		for (PipelineExecutor child : children) {
			child.prepare(stormConf, context, collector, output_tf, flexyBolt);
		}
	}

	public boolean commit(Tuple input) {
		boolean ret = true;
		
		switch (cur.getType()) {
		case FUNCTION:
		case PROJECTION:
		case SPOUT: // The ack should occur on the next release of a batch in case commit fails.
			// Do nothing.
			break;
		case GROUPBY:
			if (!cur.getIsStage0Agg()) {
				stage1Exec.commit();
			}
			break;
		default:
			throw new RuntimeException("Unknown node type:" + cur.getType());
		}
		
		// Call commit on children.
		for (PipelineExecutor child : children) {
			child.flush();
		}
		
		return ret;
	}

	public void flush() {		
		switch (cur.getType()) {
		case FUNCTION:
		case PROJECTION:
		case SPOUT:
			// Do nothing.
			break;
		case GROUPBY:
//			log.info("flush: " + cur);
			if (cur.getIsStage0Agg()) {
				stage0Exec.flush();
			} else {
				stage1Exec.flush();
			}
			break;
		default:
			throw new RuntimeException("Unknown node type:" + cur.getType());
		}
		
		// Call flush on children.
		for (PipelineExecutor child : children) {
			child.flush();
		}
	}

	private void execute(TridentTuple tup) {
//		log.info("execute: " + cur + " " + tup);

		parent = tup;

		switch (cur.getType()) {
		case FUNCTION:
			// Project as appropriate
			tup = proj_output_tf.create(tup);
			cur.getFunc().execute(tup, this);
			break;
		case GROUPBY:
			// Pull the key.
			Writable key = (Writable) tup.getValueByField(cur.getGroupingFields().get(0));
			
			// Project as appropriate
			tup = proj_output_tf.create(tup);
			if (cur.getIsStage0Agg()) {
				this.stage0Exec.execute(key, tup);
			} else {
				this.stage1Exec.execute(key, tup);
			}
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
//		log.info("execute tuple: " + input);
		boolean ret = false;
		
		switch (cur.getType()) {
		case FUNCTION:
		case GROUPBY:
		case PROJECTION:
			// Create the appropriate tuple and move along.
			execute(parent_root_tf.create(input.getValues()));
			break;
		case SPOUT:
//			log.info("execute tuple spout: " + input);
			// Check on failures
			long txid = input.getLong(0);
			boolean failed = input.getBoolean(1);
			// XXX: Assuming batch ids always increase...
			long last_txid = txid - 1;
			if(idsMap.containsKey(last_txid)) {
//				log.info("Flushing tuples: " + last_txid + " " + failed + " " + idsMap.get(last_txid).size());
				for (Object msgId : idsMap.get(last_txid)) {
					if (failed) {
						cur.getSpout().fail(msgId);
					} else {
						cur.getSpout().ack(msgId);
					}
				}
			}
//			if(idsMap.containsKey(txid)) {
//                fail(txid);
//            }
			
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
//		log.info("Emit: " + cur + " --> " + values + " --> " + exposedName);
		TridentTuple tup = null;
		// Use the appropriate output factory to create the next tuple.
		switch (cur.getType()) {
		case FUNCTION:
			tup = op_output_tf.create((TridentTupleView) parent, values);
			break;
		case GROUPBY:
			tup = root_output_tf.create(values);
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
		for (PipelineExecutor child : children) {
			child.execute(tup);
		}
		
		// Emit if necessary.
		if (exposedName != null) {
//			log.info("EMIT:" + tup);
			this.collector.emit(exposedName, tup);
		}
	}

	@Override
	public void reportError(Throwable t) {
		// TODO Auto-generated method stub
		
	}

	public static PipelineExecutor build(FStream cur, DefaultDirectedGraph<FStream, IndexedEdge<FStream>> subG) {
		ArrayList<PipelineExecutor> children = new ArrayList<PipelineExecutor>();
		for (IndexedEdge<FStream> edge : subG.outgoingEdgesOf(cur)) {
			children.add(build(edge.target, subG));
		}
		
		// TODO -- break the executors out by type vs a switch statement...
		return new PipelineExecutor(cur, children);
	}
}
