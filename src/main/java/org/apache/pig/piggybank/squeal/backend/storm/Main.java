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
import java.util.UUID;

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
import org.apache.pig.piggybank.squeal.metrics.MetricsTransportFactory;
import org.apache.pig.piggybank.squeal.metrics.TransportMeasureHelper;
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
import storm.trident.Stream;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.fluent.ChainedFullAggregatorDeclarer;
import storm.trident.fluent.ChainedPartitionAggregatorDeclarer;
import storm.trident.operation.BaseFilter;
import storm.trident.state.map.MapCombinerAggStateUpdater;
import storm.trident.tuple.TridentTuple;
import storm.trident.util.TridentUtils;

public class Main {

	protected static final String RUN_TEST_CLUSTER_KEY = "pig.streaming.run.test.cluster";
	protected static final String TEST_CLUSTER_WAIT_TIME_KEY = "pig.streaming.run.test.cluster.wait_time";
	protected static final String ACKERS_COUNT_KEY = "pig.streaming.ackers";
	protected static final String WORKERS_COUNT_KEY = "pig.streaming.workers";
	protected static final String PIG_STREAMING_KEY_PREFIX = "pig.streaming";
	protected static final String RUN_DIRECT_KEY = "pig.streaming.run.test.cluster.direct";
	protected static final String TOPOLOGY_NAME_KEY = "pig.streaming.topology.name";
	protected static final String EXTRA_CONF_KEY = "pig.streaming.extra.conf";
	protected static final String DEBUG_ENABLE_KEY = "pig.streaming.debug";
	
	protected PigContext pc;
	SOperPlan splan;
	private TridentTopology t;
	private static final Log log = LogFactory.getLog(Main.class);
	private Set<StormOper> leaves;
	
	public Main() {
		this(null, null);
	}
	
	public Main(PigContext pc, SOperPlan splan) {
		this.pc = pc;
		this.splan = splan;
		if (splan != null) {
			t = setupTopology(pc);
		}
	}
	
	public void initFromPigContext(PigContext pc) throws IOException {
		this.pc = pc;
		// Decode the plan from the context.
		splan = (SOperPlan) ObjectSerializer.deserialize(pc.getProperties().getProperty(StormLauncher.PLANKEY));
		t = setupTopology(pc);
	}
	
	static class BetterDebug extends BaseFilter {

		private String prepend;

		public BetterDebug(String prepend) {
			this.prepend = prepend;
		}
		
		@Override
		public boolean isKeep(TridentTuple tuple) {
			System.out.println("DEBUG " + prepend + ":" + tuple.toString());
	        return true;
		}
		
	}
		
	class DepWalker extends SOpPlanVisitor {

		private static final String DISABLE_SPOUT_WRAPPER_KEY = "pig.streaming.disable.spout.wrapper";
		private TridentTopology topology;
		private Map<StormOper, List<Stream>> sop_streams = new HashMap<StormOper, List<Stream>>();
		private PigContext pc;
		boolean instrumentTransport;
		
		protected DepWalker(TridentTopology topology, SOperPlan plan, PigContext pc) {
			super(plan, new DependencyOrderWalker<StormOper, SOperPlan>(plan));
			this.topology = topology;
			this.pc = pc;
			instrumentTransport = MetricsTransportFactory.hasMetricsTransport(pc.getProperties());
		}
		
		List<Stream> processMapSOP(StormOper sop) throws CloneNotSupportedException {
			Fields output_fields = sop.getOutputFields();			
			List<Stream> outputs = new ArrayList<Stream>();
			Stream output;
			
			// Cycle through the inputs and create a clone map for each.
			// This handles the cases for multiple inputs without breaking the plan apart.
			for (PhysicalOperator po : sop.getPlan().getRoots()) {
				StormOper input_sop = splan.getInputSOP((POLoad) po);
				
				List<Stream> inputs = sop_streams.get(input_sop);
				if (inputs == null) {
					// Probably a static load.
					continue;
				}
				
				for (Stream input : inputs) {
				
					if (sop.getShuffleBefore()) {
						input = input.shuffle();
					}

					// Pull and record timestamp.
					if (instrumentTransport) {
						input = TransportMeasureHelper.extractAndRecord(input, sop.name());
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
		
		List<Stream> getInputs(StormOper sop) {
			// Change to list?
			List<Stream> inputs = new ArrayList<Stream>();
			
			// Link to the previous streams.
			for (StormOper pre_sop : splan.getPredecessors(sop)) {
				inputs.addAll(sop_streams.get(pre_sop));
			}

			return inputs;
		}

		public void visitSOp(StormOper sop) throws VisitorException {
			List<Stream> outputs = new ArrayList<Stream>();
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
				Stream output = topology.newStream(sop.getOperatorKey().toString(), 
						new ImprovedRichSpoutBatchExecutor(spout_proxy));
				
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
					output = TransportMeasureHelper.instrument(output, sop.getLoadFunc().getClass().getSimpleName());
				}
				
				outputs.add(output);
				
				sop_streams.put(sop, outputs);

				return;
			}

			// Default value for non-maps.
			List<Stream> inputs = getInputs(sop);
			
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
				List<Stream> intermed = new ArrayList<Stream>();
				
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
				
				for (Stream input : inputs) {						
					System.out.println("Setting output name: " + sop.name());
					input = input.name(sop.name());

					// We need to encode the key into a value (sans index) to group properly.
					input = input.each(
								new Fields(input.getOutputFields().get(0)),
								new TriMapFunc.MakeKeyRawValue(),
								group_key
							);

					// TRACE
					//				input.each(input.getOutputFields(), new BetterDebug("TRACE1"));
					//				System.out.println("XXXXXXXX " + input.getOutputFields());

					// Partition and Aggregate.
					ChainedPartitionAggregatorDeclarer part_agg_chain = input.groupBy(group_key)
							.chainedAgg();
					
					// A uuid used to pair the two transport measurements.
					String agg_uuid = UUID.randomUUID().toString();					
					if (instrumentTransport) {
						part_agg_chain = TransportMeasureHelper.instrument_pre(part_agg_chain, sop.name(), agg_uuid);
					}
					
					part_agg_chain = part_agg_chain
							.partitionAggregate(
									orig_input_fields, 
									agg_fact.getStage1Aggregator(), 
									new Fields("stage1_vl"));

					// Handle timestamps.
					if (instrumentTransport) {
						part_agg_chain = TransportMeasureHelper.instrument(part_agg_chain, sop.name(), agg_uuid);
					}

					intermed.add(part_agg_chain.chainEnd());
				}
				
				// Merge things before doing global group by.
				Stream aggregated_part = topology.merge(intermed);
				
				// TRACE
//				aggregated_part.each(aggregated_part.getOutputFields(), new BetterDebug("TRACE2"));
//				System.out.println("XXXXXZZZ " + aggregated_part.getOutputFields());
				
				// Group and Aggregate.
				ChainedFullAggregatorDeclarer agg_chain = aggregated_part.groupBy(group_key)
						.chainedAgg();
				
				String agg_uuid = UUID.randomUUID().toString();	
				if (instrumentTransport) {
					agg_chain = TransportMeasureHelper.instrument_pre(agg_chain, sop.name(), agg_uuid);
				}
				
				agg_chain = agg_chain
							.aggregate(
									new Fields("stage1_vl"), 
									agg_fact.getStage2Aggregator(), 
									output_fields);
				
				// Handle the times after the groupBy (remote side).
				if (instrumentTransport) {
					agg_chain = TransportMeasureHelper.instrument(agg_chain, sop.name(), agg_uuid);
				}

				Stream aggregated = agg_chain.chainEnd();
				
				// TRACE
//				aggregated.each(aggregated.getOutputFields(), new BetterDebug("TRACE3"));
//				System.out.println("XXXXXYYY " + aggregated.getOutputFields());
				
				// Persist
				TridentState gr_persist = aggregated
						.partitionPersist(
									sop.getStateFactory(pc),
									TridentUtils.fieldsUnion(group_key, output_fields),
									new MapCombinerAggStateUpdater(agg_fact.getStoreAggregator(), group_key, output_fields),
									TridentUtils.fieldsConcat(group_key, output_fields)
								);
				if (sop.getParallelismHint() > 0) {
					gr_persist.parallelismHint(sop.getParallelismHint());
				}
				Stream output = gr_persist.newValuesStream();
				
				// TRACE
//				output.each(output.getOutputFields(), new BetterDebug("TRACE4"));
			
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
				
				for (Stream input : inputs) {
					// Pull and record timestamp.
					if (instrumentTransport) {
						input = TransportMeasureHelper.extractAndRecord(input, sop.name());
					}

					// Need to reduce
					Stream output = input.each(
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
					outputs.set(i, TransportMeasureHelper.instrument(outputs.get(i), sop.name()));	
				}
			}
			
			sop_streams.put(sop, outputs);
			
			System.out.println(sop.name() + " input fields: " + inputs.get(0).getOutputFields());
			System.out.println(sop.name() + " output fields: " + outputs.get(0).getOutputFields());
		}
	};
	
	public TridentTopology setupTopology(PigContext pc) {
		TridentTopology topology = new TridentTopology();
		
		// Pull out the leaves to handle storage.
		leaves = new HashSet<StormOper>(splan.getLeaves());
		
		// Walk the plan and create the topology.
		DepWalker w = new DepWalker(topology, splan, pc);
		try {
			w.visit();
		} catch (VisitorException e) {
			throw new RuntimeException(e);
		}
		
//		for (StormOper r : splan.getRoots()) {	
//			// Grab the load statement.
//			System.err.println(r);
//		}
		
		return topology;
	}
	
	protected StormTopology getTopology() {
		return t.build();
	}
	
	void explain(PrintStream ps) {
		ps.println("#--------------------------------------------------");
        ps.println("# Storm Topology                                   ");
        ps.println("#--------------------------------------------------");
        
        StormTopology topo = getTopology();
        
        ps.println("# Spouts:                                          ");
        
        Map<String, SpoutSpec> spouts = topo.get_spouts();
        for (Entry<String, SpoutSpec> ent : spouts.entrySet()) {
        	ps.println(ent.getKey() + " parallel: " + ent.getValue().get_common().get_parallelism_hint());
        	
        	SpoutSpec spec = ent.getValue();
        	
        	ps.println("Streams: ");
        	Map<String, StreamInfo> streams = spec.get_common().get_streams();
        	for (Entry<String, StreamInfo> ent2 : streams.entrySet()) {
        		ps.println("++ " + ent2.getKey() + " " + ent2.getValue());
        	}
        	
        	ps.println("#--------------------------------------------------");
        }
        
        ps.println("# Bolts:                                           ");
        
        for (Entry<String, Bolt> ent : topo.get_bolts().entrySet()) {
        	ps.println(ent.getKey() + " parallel: " + ent.getValue().get_common().get_parallelism_hint());
        	
        	ps.println("Inputs: ");
        	Map inputs = ent.getValue().get_common().get_inputs();        	
			for (Object k : inputs.keySet()) {
        		ps.println("** " + k + " " + inputs.get(k));
        	}
        	
        	ps.println("Outputs: ");
        	for (Entry<String, StreamInfo> ent2 : ent.getValue().get_common().get_streams().entrySet()) {
        		ps.println("++ " + ent2.getKey() + " " + ent2.getValue());
        	}
        	
        	ps.println("#--------------------------------------------------");
        }
	}
	
	void runTestCluster(String topology_name, long wait_time, boolean debug) {
		// Run test.
		Config conf = new Config();
		conf.put(Config.TOPOLOGY_WORKERS, 1);
		conf.put(Config.TOPOLOGY_DEBUG, debug);
		
		// Register a Serializer for any Writable.
		registerSerializer(conf);
		
		passPigContextProperties(conf);
		
		try {
			LocalCluster cluster = new LocalCluster();
			
			cluster.submitTopology(topology_name, conf, getTopology());
			
			if (wait_time > 0) {
				log.info("Waiting " + wait_time + " ms for the test cluster to run.");
				Utils.sleep(wait_time);
				cluster.killTopology(topology_name);
				cluster.shutdown();
			}
		} catch (Throwable e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
		
	}
	
	public void registerSerializer(Config conf) {
//		conf.registerSerialization(Writable.class, WritableKryoSerializer.class);
		
		// PigTypes
		conf.registerSerialization(NullableBooleanWritable.class, WritableKryoSerializer.class);
		conf.registerSerialization(NullableBytesWritable.class, WritableKryoSerializer.class);
		conf.registerSerialization(NullableText.class, WritableKryoSerializer.class);
		conf.registerSerialization(NullableFloatWritable.class, WritableKryoSerializer.class);
		conf.registerSerialization(NullableDoubleWritable.class, WritableKryoSerializer.class);
		conf.registerSerialization(NullableIntWritable.class, WritableKryoSerializer.class);
		conf.registerSerialization(NullableLongWritable.class, WritableKryoSerializer.class);
		conf.registerSerialization(NullableBag.class, WritableKryoSerializer.class);
	    conf.registerSerialization(NullableTuple.class, WritableKryoSerializer.class);
		
	    // Squeal Types
	    conf.registerSerialization(CombineWrapper.CombineWrapperState.class, WritableKryoSerializer.class);
	    conf.registerSerialization(TriBasicPersist.TriBasicPersistState.class, WritableKryoSerializer.class);
	    conf.registerSerialization(TriWindowCombinePersist.WindowCombineState.class, WritableKryoSerializer.class);
	    conf.registerSerialization(CombineTupleWritable.class, WritableKryoSerializer.class);
	}
	
	public void passPigContextProperties(Config conf) {
		for (Entry<Object, Object> prop : pc.getProperties().entrySet()) {
			String k = (String) prop.getKey();
			if (k.startsWith(PIG_STREAMING_KEY_PREFIX)) {
				conf.put(k, prop.getValue());
			}
		}
	}
	
	public void launch(String jarFile) throws AlreadyAliveException, InvalidTopologyException, IOException {
		if (pc.getProperties().getProperty(RUN_DIRECT_KEY, "false").equalsIgnoreCase("true")) {
			String topology_name = pc.getProperties().getProperty(TOPOLOGY_NAME_KEY, "PigStorm-" + pc.getLastAlias());
			runTestCluster(topology_name);
		} else {			
			// Execute "storm jar <jarfile> <this.classname>";
			String exec = "storm jar " + jarFile + " " + this.getClass().getCanonicalName();
			System.out.println("Running: " + exec);
			Process p = Runtime.getRuntime().exec(exec);
			BufferedReader sout = new BufferedReader(new InputStreamReader(p.getInputStream()));
	        BufferedReader serr = new BufferedReader(new InputStreamReader(p.getErrorStream()));
	        
	        // Pull any stdin/stdout
	        String line;
	        while ((line = sout.readLine()) != null) {
	        	System.out.println(line);
	        }
	        while ((line = serr.readLine()) != null) {
	        	System.err.println(line);
	        }
	        
	        int ret;
			try {
				ret = p.waitFor();
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
	        if (ret != 0) {
	        	throw new RuntimeException("storm jar returned with non-zero status: " + ret);
	        }
		}
	}
	
	public void submitTopology(String topology_name) throws AlreadyAliveException, InvalidTopologyException {
		Config conf = new Config();
		
		String extraConf = pc.getProperties().getProperty(EXTRA_CONF_KEY, null);
		if (extraConf != null) {
			System.out.println("Loading additional configuration properties from: " + extraConf);
			// Load the configuration file.
			Yaml yaml = new Yaml();
			FileReader fr;
			try {
				fr = new FileReader(extraConf);
				Map<String, Object> m = (Map<String, Object>) yaml.load(fr);
				conf.putAll(m);
				fr.close();
			} catch (IOException e) {
				throw new RuntimeException(e);
			}	
		}
		
		int workers = Integer.parseInt(pc.getProperties().getProperty(WORKERS_COUNT_KEY, "4"));
		conf.setNumWorkers(workers);
		int ackers = Integer.parseInt(pc.getProperties().getProperty(ACKERS_COUNT_KEY, "1"));
		conf.setNumAckers(ackers);
		
		// Register a Serializer for any Writable.
		registerSerializer(conf);
		
		passPigContextProperties(conf);
		
		StormSubmitter submitter = new StormSubmitter();
		
		submitter.submitTopology(topology_name, conf, getTopology());
	}
	
	public void runTestCluster(String topology_name) {
		log.info("Running test cluster...");
		
		boolean debug = pc.getProperties().getProperty(DEBUG_ENABLE_KEY, "false").equalsIgnoreCase("true");
		int wait_time = Integer.parseInt(pc.getProperties().getProperty(TEST_CLUSTER_WAIT_TIME_KEY, "10000"));
		
		runTestCluster(topology_name, wait_time, debug);
		
		log.info("Back from test cluster.");
	}
	
	Object getStuff(String name) {
		System.out.println(getClass().getClassLoader().getResource("pigContext"));
		ObjectInputStream fh;
		Object o = null;
		try {
			fh = new ObjectInputStream(getClass().getClassLoader().getResourceAsStream(name));
			o = fh.readObject();
			fh.close();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		return o;
	}
	
	public void runMain(String[] args) throws IOException, AlreadyAliveException, InvalidTopologyException {
		/* Create the Pig context */
		pc = (PigContext) getStuff("pigContext");
		initFromPigContext(pc);
		
		String topology_name = pc.getProperties().getProperty(TOPOLOGY_NAME_KEY, "PigStorm-" + pc.getLastAlias());
		
		if (pc.getProperties().getProperty(RUN_TEST_CLUSTER_KEY, "false").equalsIgnoreCase("true")) {
			runTestCluster(topology_name);
			log.info("Exitting forcefully due to non-terminated threads...");
			System.exit(0);
		} else {
			submitTopology(topology_name);
		}
	}
	
	public static void main(String[] args) throws Exception {
		Main m = new Main();
		m.runMain(args);
	}
}
