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
import org.apache.pig.piggybank.squeal.backend.storm.io.SpoutSource;
import org.apache.pig.piggybank.squeal.backend.storm.io.WritableKryoSerializer;
import org.apache.pig.piggybank.squeal.backend.storm.io.SpoutWrapperUtils.LimitedOutstandingInvocationHandler;
import org.apache.pig.piggybank.squeal.backend.storm.plans.SOpPlanVisitor;
import org.apache.pig.piggybank.squeal.backend.storm.plans.SOperPlan;
import org.apache.pig.piggybank.squeal.backend.storm.plans.StormOper;
import org.apache.pig.piggybank.squeal.backend.storm.state.CombineTupleWritable;
import org.apache.pig.piggybank.squeal.backend.storm.topo.FlexyBolt;
import org.apache.pig.piggybank.squeal.backend.storm.topo.FlexyMasterSpout;
import org.apache.pig.piggybank.squeal.flexy.FlexyTopology;
import org.apache.pig.piggybank.squeal.flexy.FlexyTopology.IndexedEdge;
import org.apache.pig.piggybank.squeal.flexy.executors.FlexyTracer;
import org.apache.pig.piggybank.squeal.flexy.model.FFields;
import org.apache.pig.piggybank.squeal.flexy.model.FStream;
import org.apache.pig.piggybank.squeal.flexy.oper.CombinePersist;
import org.apache.pig.piggybank.squeal.flexy.oper.CombineWrapper;
import org.apache.pig.piggybank.squeal.flexy.oper.BasicPersist;
import org.apache.pig.piggybank.squeal.flexy.oper.MapFunc;
import org.apache.pig.piggybank.squeal.flexy.oper.Reduce;
import org.apache.pig.piggybank.squeal.flexy.oper.WindowCombinePersist;
import org.apache.pig.piggybank.squeal.metrics.MetricsTransportFactory;
import org.jgrapht.graph.DefaultDirectedGraph;

import com.twitter.heron.api.HeronTopology;
import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.api.generated.TopologyAPI.Bolt;
import com.twitter.heron.api.generated.TopologyAPI.Config.KeyValue;
import com.twitter.heron.api.generated.TopologyAPI.InputStream;
import com.twitter.heron.api.generated.TopologyAPI.StreamSchema.KeyType;
import com.twitter.heron.api.generated.TopologyAPI.OutputStream;
import com.twitter.heron.api.generated.TopologyAPI.Spout;
import com.twitter.heron.api.generated.TopologyAPI.Topology;
import com.twitter.heron.api.utils.TopologyUtils;
import com.twitter.heron.common.basics.ByteAmount;
import com.twitter.heron.shaded.com.google.protobuf.Descriptors.EnumDescriptor;
import com.twitter.heron.shaded.com.google.protobuf.Descriptors.FieldDescriptor;
import com.twitter.heron.shaded.org.yaml.snakeyaml.Yaml;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.ConfigUtils;
import backtype.storm.utils.Utils;

public class Main {

	protected static final String RUN_TEST_CLUSTER_KEY = "pig.streaming.run.test.cluster";
	protected static final String TEST_CLUSTER_WAIT_TIME_KEY = "pig.streaming.run.test.cluster.wait_time";
	protected static final String ACKERS_COUNT_KEY = "pig.streaming.ackers";
	protected static final String WORKERS_COUNT_KEY = "pig.streaming.workers";
	protected static final String PIG_STREAMING_KEY_PREFIX = "pig.streaming";
	protected static final String RUN_DIRECT_KEY = "pig.streaming.run.test.cluster.direct";
	protected static final String TOPOLOGY_RUN_FORMAT_KEY = "pig.streaming.run.cluster.cmd";
	protected static final String TOPOLOGY_NAME_KEY = "pig.streaming.topology.name";
	protected static final String EXTRA_CONF_KEY = "pig.streaming.extra.conf";
	protected static final String DEBUG_ENABLE_KEY = "pig.streaming.debug";
	
	protected PigContext pc;
	SOperPlan splan;
	private static final Log log = LogFactory.getLog(Main.class);
	private Set<StormOper> leaves;
	
	private FlexyTopology ft;
	
	public Main() {
		this(null, null);
	}
	
	public Main(PigContext pc, SOperPlan splan) {
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

					log.debug("Setting output name: " + sop.name());
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
							new MapFunc(pc, clonePlan, sop.mapKeyType, sop.getIsCombined(), cloneActiveRoot, leaves.contains(sop), sop.name()),
							new FFields(output_fields.toList())
							).project(new FFields(output_fields.toList()));
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
			FFields output_fields = new FFields(sop.getOutputFields().toList());
			
			if (sop.getType() == StormOper.OpType.SPOUT) {
				// Wrap the spout to address STORM-368
				IRichSpout spout_proxy;
				if (pc.getProperties().getProperty(DISABLE_SPOUT_WRAPPER_KEY, "true").equalsIgnoreCase("true")) {
					spout_proxy = sop.getLoadFunc();
				} else {
					spout_proxy = LimitedOutstandingInvocationHandler.newInstance(sop.getLoadFunc());
				}
				
				// Use a rich spout batch executor that fixes parts of Storm-368 and tracks metrics.
				FStream output = topology.newStream(sop.getOperatorKey().toString(), new SpoutSource(spout_proxy));
				
				log.debug("Setting output name: " + sop.getLoadFunc().getClass().getSimpleName());
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
						agg_fact = new CombineWrapper.Factory(new BasicPersist());
					} else {
						// We'll be windowing things.
						agg_fact = new CombineWrapper.Factory(new WindowCombinePersist(sop.getWindowOptions()));
					}
				} else {					
					// We need to trim things from the plan re:PigCombiner.java
					POPackage pack = (POPackage) sop.getPlan().getRoots().get(0);
					sop.getPlan().remove(pack);

					agg_fact = new CombineWrapper.Factory(new CombinePersist(pack, sop.getPlan(), sop.mapKeyType));
				}
				
				FFields orig_input_fields = inputs.get(0).getOutputFields();
				FFields group_key = new FFields(orig_input_fields.get(0) + "_raw");
				
				for (FStream input : inputs) {						
//					System.out.println("Setting output name: " + sop.name());
//					input = input.name(sop.name());

					// We need to encode the key into a value (sans index) to group properly.
					input = input.each(
								new FFields(input.getOutputFields().get(0)),
								new MapFunc.MakeKeyRawValue(),
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

				log.debug("Setting output name: " + sop.name());
				output = output.name(sop.name());
				
				// Re-alias the raw as the key.
				output = output.each(
							group_key,
							new MapFunc.Copy(),
							new FFields(orig_input_fields.get(0))
						);

				// Strip down to the appropriate values
				output = output.project(new FFields(orig_input_fields.get(0), output_fields.get(0)));
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
							new Reduce(pc, sop.getPlan(), false, leaves.contains(sop), sop.name()), 
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
			
			log.debug(sop.name() + " input fields: " + inputs.get(0).getOutputFields());
			log.debug(sop.name() + " output fields: " + outputs.get(0).getOutputFields());
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
	
	void _visitAndBuild(FlexyBolt b, Set<FlexyBolt> built_memo, TopologyBuilder builder) {
		if (built_memo.contains(b)) {
			return;
		}
		
		// Add the bolt to the topology.
		BoltDeclarer b_builder = builder.setBolt(b.getName(), b, b.getParallelism());
		
		// Link to the appropriate inputs.
		if (b.getRoot().getType() == FStream.NodeType.SPOUT) {
			// Link to the master.
			b_builder.allGrouping("FlexyMaster", "start");
			b_builder.allGrouping("FlexyMaster", "commit");
		}
		
		Set<String> sourceBolts = new HashSet<String>();
		
		for (IndexedEdge<FStream> edge : ft.getBoltG().incomingEdgesOf(b)) {
			// Ensure it exists.
			FlexyBolt source_b = ft.getBoltMap().get(edge.source);
			_visitAndBuild(source_b, built_memo, builder);
			
			// Now link it.
			String source_name = source_b.getName();
			sourceBolts.add(source_name);
			String source_stream = source_b.getStreamName(edge.source);
			
			// Pull the schema of the input.
			b.setInputSchema(edge.source.getOutputFields());

//			log.error("Bolt:" + b + " source: " + source_name + " source_stream: " + source_stream);
//			System.err.println("XXXXXXX -- Bolt:" + b + " source: " + source_name + " source_stream: " + source_stream + " --- " + b.getRoot().getType() + " " + b.getInputSchema());
			if (b.getRoot().getType() == FStream.NodeType.SHUFFLE) {
				b_builder.shuffleGrouping(source_name, source_stream);
			} else if (b.getRoot().getType() == FStream.NodeType.GROUPBY) {
				b_builder.fieldsGrouping(source_name, source_stream, new Fields(b.getRoot().getGroupingFields().toList()));
			}
		}
		
		for (String source_name : sourceBolts) {
			// Subscribe to the coordination stream
			b_builder.allGrouping(source_name, "coord");
		}
	}

	
	protected StormTopology getTopology() {
		// Crawl the graph and create execution pipelines to be run in the bolts.
		ft.logicalToBoltGraph();

		// Now, convert the bolt graph to a topology.		
		TopologyBuilder builder = new TopologyBuilder();

		// Create the coordinator spout.
		builder.setSpout("FlexyMaster", new FlexyMasterSpout());

		// Start from the leaves and walk to the spouts using DFS.
		Set<FlexyBolt> built_memo = new HashSet<FlexyBolt>();
		DefaultDirectedGraph<FlexyBolt, IndexedEdge<FStream>> boltG = ft.getBoltG();
		for (FlexyBolt b : boltG.vertexSet()) {
			if (boltG.outDegreeOf(b) == 0) {
				_visitAndBuild(b, built_memo, builder);
			}
		}

		return builder.createTopology();
	}
	
	List<String> keyListToList(List<KeyType> kl) {
		List<String> fieldStrings = new ArrayList<String>(kl.size());
		for (KeyType s : kl) {
			fieldStrings.add(s.getKey());
		}
		return fieldStrings;
	}
	
	void explain(PrintStream ps) {
		ps.println("#--------------------------------------------------");
        ps.println("# Storm Topology                                   ");
        ps.println("#--------------------------------------------------");
        
        StormTopology topo = getTopology();
        
        ps.println("# Spouts:                                          ");
        
        // Storm config, use an empty map to see what that does.
        com.twitter.heron.api.Config heronConfig = ConfigUtils.translateConfig(new HashMap());
        
        Topology htopo = topo.getStormTopology()
        		.setConfig(heronConfig).
                setName("MAIN-explain").
                setState(TopologyAPI.TopologyState.RUNNING).
                getTopology();
               
		for (Spout ent : htopo.getSpoutsList()) {
			
        	ps.println(ent.getComp().getName() + " parallel: " + TopologyUtils.getConfigWithDefault(
        			ent.getComp().getConfig().getKvsList(), com.twitter.heron.api.Config.TOPOLOGY_COMPONENT_PARALLELISM, ""));

        	ps.println("Streams: ");
        	
        	for (OutputStream output : ent.getOutputsList()) {
        		ps.println("++ " + output.getStream().getId() + " " + keyListToList(output.getSchema().getKeysList()));        		
        	}
        	
        	ps.println("#--------------------------------------------------");
        }
        
        ps.println("# Bolts:                                           ");
        
        for (Bolt bolt : htopo.getBoltsList()) {
        	ps.println(bolt.getComp().getName() + " parallel: " + TopologyUtils.getConfigWithDefault(
        			bolt.getComp().getConfig().getKvsList(), com.twitter.heron.api.Config.TOPOLOGY_COMPONENT_PARALLELISM, ""));
        	
        	ps.println("Inputs: ");
			for (InputStream k : bolt.getInputsList()) {
        		ps.println("** " + k.getStream().getComponentName() + " " + k.getStream().getId() + " " + k.getGtype() + " " + keyListToList(k.getGroupingFields().getKeysList()));
        	}
        	
        	ps.println("Outputs: ");
        	for (OutputStream out : bolt.getOutputsList()) {
        		ps.println("++ " + out.getStream().getId() + " " + keyListToList(out.getSchema().getKeysList()));
        	}
        	
        	ps.println("#--------------------------------------------------");        	
        }
	}
	
	void runTestCluster(String topology_name, long wait_time, boolean debug) {
		// Run test.
		Config conf = new Config();
		conf.put(Config.TOPOLOGY_WORKERS, 1);
		conf.put(Config.TOPOLOGY_DEBUG, debug);
		conf.setNumAckers(1);
		
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
	    conf.registerSerialization(BasicPersist.TriBasicPersistState.class, WritableKryoSerializer.class);
	    conf.registerSerialization(WindowCombinePersist.WindowCombineState.class, WritableKryoSerializer.class);
	    conf.registerSerialization(CombineTupleWritable.class, WritableKryoSerializer.class);
	    conf.registerSerialization(FlexyTracer.class, FlexyTracer.TracerKryoSerializer.class);
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
			String topology_name = getTopologyName();
			runTestCluster(topology_name);
		} else {
			// Determine if there is a format property.
			String runFormat = pc.getProperties().getProperty(TOPOLOGY_RUN_FORMAT_KEY, "storm jar %s %s");
			
			// Execute "storm jar <jarfile> <this.classname>";
			String exec = String.format(runFormat, jarFile, this.getClass().getCanonicalName());
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
	        	throw new RuntimeException(String.format("%s returned with non-zero status: %s", exec, ret));
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
		
		// FIXME: Make this flexible...
		//com.twitter.heron.api.Config.setContainerRamRequested(conf, ByteAmount.fromGigabytes(1));
		
		int workers = Integer.parseInt(pc.getProperties().getProperty(WORKERS_COUNT_KEY, "4"));
		conf.setNumWorkers(workers);
		int ackers = Integer.parseInt(pc.getProperties().getProperty(ACKERS_COUNT_KEY, "1"));
		conf.setNumAckers(ackers);
		
		// Register a Serializer for any Writable.
		registerSerializer(conf);
		
		System.out.println("Submitting with config: " + conf);
		
		passPigContextProperties(conf);
		
		StormSubmitter.submitTopology(topology_name, conf, getTopology());
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
	
	public String getTopologyName() {
		return pc.getProperties().getProperty(TOPOLOGY_NAME_KEY, "pigsqueal-" + pc.getLastAlias()).toLowerCase();
	}
	
	public void runMain(String[] args) throws IOException, AlreadyAliveException, InvalidTopologyException {
		/* Create the Pig context */
		pc = (PigContext) getStuff("pigContext");
		initFromPigContext(pc);
		
		String topology_name = getTopologyName();
		
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
