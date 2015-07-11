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

package org.apache.pig.piggybank.squeal.backend.storm;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLoad;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POPackage;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.NullableBag;
import org.apache.pig.impl.io.NullableBooleanWritable;
import org.apache.pig.impl.io.NullableBytesWritable;
import org.apache.pig.impl.io.NullableDoubleWritable;
import org.apache.pig.impl.io.NullableFloatWritable;
import org.apache.pig.impl.io.NullableIntWritable;
import org.apache.pig.impl.io.NullableLongWritable;
import org.apache.pig.impl.io.NullableText;
import org.apache.pig.impl.io.NullableTuple;
import org.apache.pig.impl.plan.DependencyOrderWalker;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.util.MultiMap;
import org.apache.pig.impl.util.ObjectSerializer;
import org.apache.pig.piggybank.squeal.backend.storm.io.ImprovedRichSpoutBatchExecutor;
import org.apache.pig.piggybank.squeal.backend.storm.io.SpoutWrapperUtils.LimitedOutstandingInvocationHandler;
import org.apache.pig.piggybank.squeal.backend.storm.io.WritableKryoSerializer;
import org.apache.pig.piggybank.squeal.backend.storm.oper.CombineWrapper;
import org.apache.pig.piggybank.squeal.backend.storm.oper.TriBasicPersist;
import org.apache.pig.piggybank.squeal.backend.storm.oper.TriCombinePersist;
import org.apache.pig.piggybank.squeal.backend.storm.oper.TriMapFunc;
import org.apache.pig.piggybank.squeal.backend.storm.oper.TriReduce;
import org.apache.pig.piggybank.squeal.backend.storm.oper.TriWindowCombinePersist;
import org.apache.pig.piggybank.squeal.backend.storm.plans.SOpPlanVisitor;
import org.apache.pig.piggybank.squeal.backend.storm.plans.SOperPlan;
import org.apache.pig.piggybank.squeal.backend.storm.plans.StormOper;
import org.apache.pig.piggybank.squeal.backend.storm.state.CombineTupleWritable;
import org.apache.pig.piggybank.squeal.flexy.FlexyTopology;
import org.apache.pig.piggybank.squeal.flexy.executors.FlexyTracer;
import org.apache.pig.piggybank.squeal.flexy.model.FStream;
import org.apache.pig.piggybank.squeal.metrics.MetricsTransportFactory;
import org.yaml.snakeyaml.Yaml;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.Bolt;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.SpoutSpec;
import backtype.storm.generated.StormTopology;
import backtype.storm.generated.StreamInfo;
import backtype.storm.topology.IRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import storm.trident.operation.BaseFilter;
import storm.trident.tuple.TridentTuple;
import storm.trident.util.TridentUtils;

public class FlexyMain extends Main {
	
	SOperPlan splan;
	private FlexyTopology ft;
	private static final Log log = LogFactory.getLog(FlexyMain.class);
	private Set<StormOper> leaves;
	
	public FlexyMain() {
		this(null, null);
	}
	
	public FlexyMain(PigContext pc, SOperPlan splan) {
		this.pc = pc;
		this.splan = splan;
		if (splan != null) {
			ft = setupFlexyTopology(pc);
		}
	}
	
	public void initFromPigContext(PigContext pc) throws IOException {
		this.pc = pc;
		// Decode the plan from the context.
		splan = (SOperPlan) ObjectSerializer.deserialize(pc.getProperties().getProperty(StormLauncher.PLANKEY));
		ft = setupFlexyTopology(pc);
	}
		
	class DepWalker extends SOpPlanVisitor {

		private static final String DISABLE_SPOUT_WRAPPER_KEY = "pig.streaming.disable.spout.wrapper";
		private FlexyTopology topology;
		private Map<StormOper, List<FStream>> sop_streams = new HashMap<StormOper, List<FStream>>();
		private PigContext pc;
		boolean instrumentTransport;
		
		protected DepWalker(FlexyTopology topology, SOperPlan plan, PigContext pc) {
			super(plan, new DependencyOrderWalker<StormOper, SOperPlan>(plan));
			this.topology = topology;
			this.pc = pc;
			instrumentTransport = MetricsTransportFactory.hasMetricsTransport(pc.getProperties());
		}
		
		List<FStream> processMapSOP(StormOper sop) throws CloneNotSupportedException {
			Fields output_fields = sop.getOutputFields();			
			List<FStream> outputs = new ArrayList<FStream>();
			FStream output;
			
			// Cycle through the inputs and create a clone map for each.
			// This handles the cases for multiple inputs without breaking the plan apart.
			for (PhysicalOperator po : sop.getPlan().getRoots()) {
				StormOper input_sop = splan.getInputSOP((POLoad) po);
				
				List<FStream> inputs = sop_streams.get(input_sop);
				if (inputs == null) {
					// Probably a static load.
					continue;
				}
				
				for (FStream input : inputs) {
				
					if (sop.getShuffleBefore()) {
						input = input.shuffle();
					}

					// Pull and record timestamp.
					if (instrumentTransport) {
						// FIXME
//						input = TransportMeasureHelper.extractAndRecord(input, sop.name());
					}

					System.out.println("Setting output name: " + sop.name());
					input = input.name(sop.name());

					MultiMap<PhysicalOperator, PhysicalOperator> opmap = new MultiMap<PhysicalOperator, PhysicalOperator>();
					sop.getPlan().setOpMap(opmap);
					PhysicalPlan clonePlan = sop.getPlan().clone();
					if (opmap.get(po).size() > 1) {
						throw new RuntimeException("Didn't expect activeRoot to have multiple values in cloned plan!");
					}
					PhysicalOperator cloneActiveRoot = opmap.get(po).get(0);

					//				System.out.println("processMapSOP -- input: " + input + " " + input_sop + " " + po);
					output = input.each(
							input.getOutputFields(),
							new TriMapFunc(pc, clonePlan, sop.mapKeyType, sop.getIsCombined(), cloneActiveRoot, leaves.contains(sop), sop.name()),
							output_fields
							).project(output_fields);
					outputs.add(output);

					if (sop.getParallelismHint() != 0) {
						output.parallelismHint(sop.getParallelismHint());
					}
				}
			}

//			// Optional debug.
////		output.each(output.getOutputFields(), new Debug());

			
			return outputs;
		}
		
		List<FStream> getInputs(StormOper sop) {
			// Change to list?
			List<FStream> inputs = new ArrayList<FStream>();
			
			// Link to the previous streams.
			for (StormOper pre_sop : splan.getPredecessors(sop)) {
				inputs.addAll(sop_streams.get(pre_sop));
			}

			return inputs;
		}

		public void visitSOp(StormOper sop) throws VisitorException {
			List<FStream> outputs = new ArrayList<FStream>();
			Fields output_fields = sop.getOutputFields();
			
			if (sop.getType() == StormOper.OpType.SPOUT) {
				// Wrap the spout to address STORM-368
				IRichSpout spout_proxy;
				if (pc.getProperties().getProperty(DISABLE_SPOUT_WRAPPER_KEY, "true").equalsIgnoreCase("true")) {
					spout_proxy = sop.getLoadFunc();
				} else {
					spout_proxy = LimitedOutstandingInvocationHandler.newInstance(sop.getLoadFunc());
				}
				
				// Use a rich spout batch executor that fixes parts of Storm-368 and tracks metrics.
				FStream output = topology.newStream(sop.getOperatorKey().toString(), spout_proxy);
				
				System.out.println("Setting output name: " + sop.getLoadFunc().getClass().getSimpleName());
				output = output.name(sop.getLoadFunc().getClass().getSimpleName());
				
				// Allow more than one to run.
				if (sop.getParallelismHint() != 0) {
					output.parallelismHint(sop.getParallelismHint());
				}
				
				// Add the conversion routine to the end to switch from Storm to Pig tuples.
				output = output.each(
							output.getOutputFields(),
							sop.getTupleConverter(),
							output_fields)
						.project(output_fields);
				
//				output.each(output.getOutputFields(), new Debug());
				
				// Add source and timestamp.
				if (instrumentTransport) {
					//FIXME: Implement this...
//					output = TransportMeasureHelper.instrument(output, sop.getLoadFunc().getClass().getSimpleName());
				}
				
				outputs.add(output);
				
				sop_streams.put(sop, outputs);

				return;
			}

			// Default value for non-maps.
			List<FStream> inputs = getInputs(sop);
			
			// Create the current operator on the topology
			if (sop.getType() == StormOper.OpType.MAP) {
				try {
					outputs = processMapSOP(sop);
//					output.each(output.getOutputFields(), new Debug());
				} catch (CloneNotSupportedException e) {
					throw new RuntimeException(e);
				}
			} else if (sop.getType() == StormOper.OpType.BASIC_PERSIST || sop.getType() == StormOper.OpType.COMBINE_PERSIST) {
				// Accumulate streams before groupby.
				List<FStream> intermed = new ArrayList<FStream>();
				
				// Setup the aggregator.
				// We want one aggregator to handle the actual combine on a partitioned stream.
				// We want this one to keep track of LAST -- it's used with storage and right before reducedelta.
				CombineWrapper.Factory agg_fact = null;				
				if (sop.getType() == StormOper.OpType.BASIC_PERSIST) {
					if (sop.getWindowOptions() == null) {
						agg_fact = new CombineWrapper.Factory(new TriBasicPersist());
					} else {
						// We'll be windowing things.
						agg_fact = new CombineWrapper.Factory(new TriWindowCombinePersist(sop.getWindowOptions()));
					}
				} else {					
					// We need to trim things from the plan re:PigCombiner.java
					POPackage pack = (POPackage) sop.getPlan().getRoots().get(0);
					sop.getPlan().remove(pack);

					agg_fact = new CombineWrapper.Factory(new TriCombinePersist(pack, sop.getPlan(), sop.mapKeyType));
				}
				
				Fields orig_input_fields = inputs.get(0).getOutputFields();
				Fields group_key = new Fields(orig_input_fields.get(0) + "_raw");
				
				for (FStream input : inputs) {						
					System.out.println("Setting output name: " + sop.name());
					input = input.name(sop.name());

					// We need to encode the key into a value (sans index) to group properly.
					input = input.each(
								new Fields(input.getOutputFields().get(0)),
								new TriMapFunc.MakeKeyRawValue(),
								group_key
							);
					
					intermed.add(input);
				}
				
				// Merge things before doing global group by.
				FStream merged_input = topology.merge(intermed);
				
				// TRACE
//				aggregated_part.each(aggregated_part.getOutputFields(), new BetterDebug("TRACE2"));
//				System.out.println("XXXXXZZZ " + aggregated_part.getOutputFields());
				
				// Group and Aggregate.
				FStream output = merged_input.groupBy(
						group_key,
						orig_input_fields,
						agg_fact.getStage1Aggregator(), 
						agg_fact.getStage2Aggregator(), 
						agg_fact.getStoreAggregator(),
						sop.getStateFactory(pc),
						output_fields);
				
				if (sop.getParallelismHint() > 0) {
					output.parallelismHint(sop.getParallelismHint());
				}

				// Re-alias the raw as the key.
				output = output.each(
							group_key,
							new TriMapFunc.Copy(),
							new Fields(orig_input_fields.get(0))
						);

				// Strip down to the appropriate values
				output = output.project(new Fields(orig_input_fields.get(0), output_fields.get(0)));
//				output.each(output.getOutputFields(), new Debug());
				
				outputs.add(output);
			} else if (sop.getType() == StormOper.OpType.REDUCE_DELTA) {
				
				for (FStream input : inputs) {
					// Pull and record timestamp.
					if (instrumentTransport) {
//						FIXME: Implement this.
//						input = TransportMeasureHelper.extractAndRecord(input, sop.name());
					}

					// Need to reduce
					FStream output = input.each(
							input.getOutputFields(), 
							new TriReduce(pc, sop.getPlan(), false, leaves.contains(sop), sop.name()), 
							output_fields
							).project(output_fields);
					//output.each(output.getOutputFields(), new Debug());
					outputs.add(output);
				}
			}
			
			// Add source and timestamp.
			if (instrumentTransport) {
				for (int i = 0; i < outputs.size(); i++) {
//					FIXME: Instrument this
//					outputs.set(i, TransportMeasureHelper.instrument(outputs.get(i), sop.name()));	
				}
			}
			
			sop_streams.put(sop, outputs);
			
			System.out.println(sop.name() + " input fields: " + inputs.get(0).getOutputFields());
			System.out.println(sop.name() + " output fields: " + outputs.get(0).getOutputFields());
		}
	};
	
	public FlexyTopology setupFlexyTopology(PigContext pc) {
		FlexyTopology topology = new FlexyTopology();
		
		// Pull out the leaves to handle storage.
		leaves = new HashSet<StormOper>(splan.getLeaves());
		
		// Walk the plan and create the topology.
		DepWalker w = new DepWalker(topology, splan, pc);
		try {
			w.visit();
		} catch (VisitorException e) {
			throw new RuntimeException(e);
		}
		
		return topology;
	}
	
	protected StormTopology getTopology() {
		return ft.build();
	}
	
	public void registerSerializer(Config conf) {
		super.registerSerializer(conf);
		
		// Squeal Types
	    conf.registerSerialization(FlexyTracer.class, FlexyTracer.TracerKryoSerializer.class);
	}
	
	public static void main(String[] args) throws Exception {
		FlexyMain m = new FlexyMain();
		m.runMain(args);
	}
}
