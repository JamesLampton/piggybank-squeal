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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Writable;
import org.apache.pig.piggybank.squeal.backend.storm.io.CaptureCollector;
import org.apache.pig.piggybank.squeal.binner.Binner;
import org.apache.pig.piggybank.squeal.binner.Binner.BinDecoder;
import org.apache.pig.piggybank.squeal.flexy.FlexyTopology.IndexedEdge;
import org.apache.pig.piggybank.squeal.flexy.components.ICollector;
import org.apache.pig.piggybank.squeal.flexy.components.IFlexyTuple;
import org.apache.pig.piggybank.squeal.flexy.components.IOutputCollector;
import org.apache.pig.piggybank.squeal.flexy.components.IRunContext;
import org.apache.pig.piggybank.squeal.flexy.components.SourceOutputCollector;
import org.apache.pig.piggybank.squeal.flexy.components.impl.FlexyTupleFactory;
import org.apache.pig.piggybank.squeal.flexy.model.FStream;
import org.apache.pig.piggybank.squeal.flexy.model.FStream.NodeType;
import org.jgrapht.graph.DefaultDirectedGraph;

public class PipelineExecutor implements ICollector {
	private FStream cur;
	private List<PipelineExecutor> children;
	private IRunContext context;
	private static final Log log = LogFactory.getLog(PipelineExecutor.class);

	// Spout stuff
	CaptureCollector _collector = new CaptureCollector();
	private int maxBatchSize;
	public static final String MAX_BATCH_SIZE_CONF = "topology.spout.max.batch.size";
	static public final int DEFAULT_MAX_BATCH_SIZE = 1000;

	// Assume single active batch at this time.
	private Map<Long, List<Object>> idsMap = new HashMap<Long, List<Object>>();
	private FlexyTupleFactory op_output_tf;
	private FlexyTupleFactory proj_output_tf;
	private FlexyTupleFactory root_output_tf;
	private IFlexyTuple parent;
	private String exposedName;
	private Stage0Executor stage0Exec;
	private Stage1Executor stage1Exec;
	private FlexyTupleFactory parent_root_tf;
	private Binner binner;
	private BinDecoder binDecoder;
	private Object anchor;
	private int emptyStreak = 0;

	private PipelineExecutor(FStream cur, List<PipelineExecutor> children) {
		this.cur = cur;
		this.children = children;
	}

	public void prepare(IRunContext context, IOutputCollector collector) {
		prepare(context, collector, null);
	}

	private void prepare(IRunContext context, 
			IOutputCollector collector, FlexyTupleFactory parent_tf) {			

		this.context = context;

		exposedName = context.getExposedName(cur);
		if (exposedName != null) {
			// Initialize and prepare a Binner.
			binner = new Binner();
			binner.prepare(context, collector, exposedName);
		}

		binDecoder = new Binner.BinDecoder();

		if (parent_tf == null && cur.getType() != NodeType.SPOUT) {
//			log.info("NULL tf: " + cur + " " + flexyBolt.getInputSchema());
			parent_tf = parent_root_tf = FlexyTupleFactory.newFreshOutputFactory(context.getInputSchema());
		}

		FlexyTupleFactory output_tf;
		// Create an output tuple factory.
		switch (cur.getType()) {
		case FUNCTION:
			cur.getFunc().prepare(context);
			// Create a projection for the input.
			proj_output_tf = FlexyTupleFactory.newProjectionFactory(cur.getInputFields());
			op_output_tf = FlexyTupleFactory.newOperationOutputFactory(parent_tf, cur.getAppendOutputFields());
			output_tf = op_output_tf;
			break;
		case GROUPBY:
			proj_output_tf = FlexyTupleFactory.newProjectionFactory(cur.getInputFields());
			root_output_tf = FlexyTupleFactory.newFreshOutputFactory(cur.getOutputFields());
			output_tf = root_output_tf;

			// Prepare the agg stuff.
			if (cur.getIsStage0Agg()) {
				this.stage0Exec = new Stage0Executor(cur.getStage0Agg());
				stage0Exec.prepare(context, this);
			} else {
				this.stage1Exec = new Stage1Executor(cur.getStage1Agg(), cur.getStorageAgg(), cur.getStateFactory());
				stage1Exec.prepare(context, this);
			}

			break;
		case PROJECTION:
			proj_output_tf = FlexyTupleFactory.newProjectionFactory(cur.getAppendOutputFields());
			output_tf = proj_output_tf;
			break;
		case SPOUT:
			Number batchSize = (Number) context.get(MAX_BATCH_SIZE_CONF);
			if(batchSize==null) batchSize = DEFAULT_MAX_BATCH_SIZE;
			maxBatchSize = batchSize.intValue();

			// Prepare the spout
			cur.getSource().open(context, new SourceOutputCollector(_collector));

			root_output_tf = FlexyTupleFactory.newFreshOutputFactory(cur.getAppendOutputFields());
			output_tf = root_output_tf;
			
			break;
		case SHUFFLE:
			// Do nothing, we'll pass directly to children later.
			output_tf = null;
			break;
		default:
			throw new RuntimeException("Unknown node type:" + cur.getType());
		}

		for (PipelineExecutor child : children) {
			child.prepare(context, collector, output_tf);
		}
	}

	public boolean commit(long txid) {
		boolean ret = true;

		switch (cur.getType()) {
		case FUNCTION:
		case PROJECTION:
		case SHUFFLE:
		case SPOUT: // The ack should occur on the next release of a batch in case commit fails.
			// Do nothing.
			break;
		case GROUPBY:
			if (!cur.getIsStage0Agg()) {
				stage1Exec.commit(txid);
			}
			break;
		default:
			throw new RuntimeException("Unknown node type:" + cur.getType());
		}

		// Call commit on children.
		for (PipelineExecutor child : children) {
			child.commit(txid);
		}

		return ret;
	}

	public void flush(Object inputAnchor) {	
		anchor = inputAnchor;
		
		switch (cur.getType()) {
		case FUNCTION:
		case PROJECTION:
		case SHUFFLE:
		case SPOUT:
			// Do nothing.
			break;
		case GROUPBY:
//			System.err.println("PipeLineExecutor.flush: " + cur);
			if (cur.getIsStage0Agg()) {
				stage0Exec.flush();
			} else {
				stage1Exec.flush();
			}
			break;
		default:
			throw new RuntimeException("Unknown node type:" + cur.getType());
		}

		// Flush any outstanding messages.
		if (exposedName != null) {
			binner.flush(inputAnchor);
		}

		// Call flush on children.
		for (PipelineExecutor child : children) {
			child.flush(inputAnchor);
		}

		anchor = null;
	}

	private void execute(IFlexyTuple tup, Object anchor) {
//		log.info("Pipeline.execute: " + cur + " " + tup);

		this.anchor = anchor;
		try {
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
		} finally {
			this.anchor = null;
		}
	}

	public boolean execute(IFlexyTuple input) {
//		System.out.println(cur.getName() + " execute tuple: " + input);
		boolean ret = false;

		switch (cur.getType()) {
		case SHUFFLE:
			// Pass through to children.
			for (PipelineExecutor child : children) {
				child.execute(input);
			}
			break;
		case FUNCTION:
		case GROUPBY:
		case PROJECTION:
			// Decode the tuples within the bin.
			List<Object> list;
			try {
				binDecoder.reset(input.getBinary(1));
				while (null != (list = binDecoder.decodeList())) {
//					System.out.println("execute decoded: " + list);
					// Create the appropriate tuple and move along.
					execute(parent_root_tf.create(list), input);
				}
			} catch (IOException e1) {
				throw new RuntimeException(e1);
			}

			break;
		case SPOUT:
			this.anchor = input;
			try {
				//			log.info("execute tuple spout: " + input);
				// Check on failures
				long txid = input.getLong(0);
				boolean failed = input.getBoolean(1);
				// XXX: Assuming batch ids always increase...
				long last_txid = txid - 1;
				if(idsMap.containsKey(last_txid)) {
					if (failed && idsMap.get(last_txid).size() > 0) { 
//						log.info("Flushing tuples: " + last_txid + " " + failed + " " + idsMap.get(last_txid).size());
					}
					for (Object msgId : idsMap.remove(last_txid)) {
						if (failed) {
							cur.getSource().fail(msgId);
						} else {
							cur.getSource().ack(msgId);
						}
					}
				}
				//			if(idsMap.containsKey(txid)) {
				//                fail(txid);
				//            }

				// Release some tuples.
				_collector.reset(this);
				Exception spoutException = null;
				for(int i=0; i < maxBatchSize; i++) {
					try {
						cur.getSource().nextTuple();
					} catch (Exception e) {
						// Delay this until we have added the emitted ids to the idsMap.
						spoutException = e;
						break;
					}
					if(_collector.numEmitted < i) {
						break;
					}
				}
				
				if (_collector.numEmitted == 0) {
					emptyStreak ++;
					// Wait if necessary.
					context.runWaitStrategy(emptyStreak);
				} else {
					emptyStreak = 0;
				}

				// Save off the emitted ids.
				idsMap.put(txid, _collector.ids);

				if (spoutException != null) {
					// Fail the ids.
					for (Object msgId : idsMap.remove(txid)) {
						cur.getSource().fail(msgId);
					}
					throw new RuntimeException(spoutException);
				}
			} finally {
				anchor = null;
			}

			ret = true;
			break;
		default:
			throw new RuntimeException("Unknown node type:" + cur.getType());
		}

		return ret;
	}

	@Override
	public void emit(List<Object> values) {
//		System.out.println("PipelineExecutor.emit: " + cur + " --> " + values + " --> " + exposedName);
		IFlexyTuple tup = null;
		// Use the appropriate output factory to create the next tuple.
		switch (cur.getType()) {
		case FUNCTION:
			tup = op_output_tf.create(parent, values);
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
			child.execute(tup, anchor);
		}

		// Emit if necessary.
		if (exposedName != null) {
			try {
				binner.emit(tup, anchor);
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
	}

	@Override
	public void reportError(Throwable t) {
		t.printStackTrace();
		// Let the flexy bolt clean this mess up.
		throw new RuntimeException(t);
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
