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
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
import org.apache.pig.impl.util.ObjectSerializer;
import org.apache.pig.piggybank.squeal.backend.storm.io.WritableKryoSerializer;
import org.apache.pig.piggybank.squeal.backend.storm.plans.SOperPlan;
import org.apache.pig.piggybank.squeal.backend.storm.plans.StormOper;
import org.apache.pig.piggybank.squeal.backend.storm.state.CombineTupleWritable;
import org.apache.pig.piggybank.squeal.flexy.oper.CombineWrapper;
import org.apache.pig.piggybank.squeal.flexy.oper.BasicPersist;
import org.apache.pig.piggybank.squeal.flexy.oper.WindowCombinePersist;
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
import backtype.storm.utils.Utils;
import storm.trident.TridentTopology;
import storm.trident.operation.BaseFilter;
import storm.trident.tuple.TridentTuple;

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

	public TridentTopology setupTopology(PigContext pc) {
		
		return null;
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
	    conf.registerSerialization(BasicPersist.TriBasicPersistState.class, WritableKryoSerializer.class);
	    conf.registerSerialization(WindowCombinePersist.WindowCombineState.class, WritableKryoSerializer.class);
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
