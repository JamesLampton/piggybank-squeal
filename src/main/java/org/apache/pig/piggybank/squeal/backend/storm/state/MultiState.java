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

package org.apache.pig.piggybank.squeal.backend.storm.state;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.Writable;
import org.apache.pig.impl.util.Pair;
import org.apache.pig.piggybank.squeal.flexy.components.IMapState;
import org.apache.pig.piggybank.squeal.flexy.components.IRunContext;
import org.apache.pig.piggybank.squeal.flexy.components.IStateFactory;

import backtype.storm.task.IMetricsContext;

public class MultiState implements IMapState<IPigIdxState> {

	private IMapState def_state;
	private Map<Integer, IMapState> idx_map;
	private ArrayList<IMapState> maps ;
	List<Integer[]> bins;

	private MultiState(IMapState def_state, Map<Integer, IMapState> idx_map, ArrayList<IMapState> maps, ArrayList<Integer[]> bins_in) {
		this.def_state = def_state;
		this.idx_map = idx_map;
		this.bins = bins_in;
		this.maps = maps;
	}
	
	public static IStateFactory fromJSONArgs(HashMap args) {
		return new Factory(args);
	}
	
	public static class Factory implements IStateFactory, IUDFExposer {
		Map<Integer, Integer> opts_map = new HashMap<Integer, Integer>();
		IStateFactory def_fact;
		private ArrayList<Integer[]> bins_in;
		private ArrayList<IStateFactory> state_facts;
		static final Integer[] dummy = new Integer[0];
		
		public String toString() {
			return "MultiState(default: " + def_fact + ", " + state_facts + ", " + opts_map  + ")";
		}
		
		public Factory(HashMap args) {
//			System.out.println("MultiState.Factory: " + args);
			bins_in = new ArrayList<Integer[]>();
			state_facts = new ArrayList<IStateFactory>();
			
			// Parse the args.
			for (Object k : args.keySet()) {
//				System.out.println("MultiState.Factory: " + k + " -> " + args.get(k));
				Map opts = (Map) args.get(k);
				IStateFactory k_fact = StateWrapper.getStateFactoryFromArgs(
						(String) opts.get("StateFactory"), 
						(String) opts.get("StaticMethod"), 
						(Object[]) opts.get("args"));
				
				if (k.equals("default")) {
					def_fact = k_fact;
				} else {
					ArrayList<Integer> cur_bins = new ArrayList<Integer>();
					for (String sub : ((String) k).split(",")) {
						Integer b = Integer.parseInt(sub);
						opts_map.put(b, state_facts.size());
						cur_bins.add(b);
					}
					bins_in.add(cur_bins.toArray(dummy));
					state_facts.add(k_fact);
				}
			}
		}
		
		@Override
		public IMapState makeState(IRunContext context) {
			//Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
			IMapState def_state = null;
			if (def_fact != null) {
				def_state = (IMapState) def_fact.makeState(context);
			}
			
			Map<Integer, IMapState> idx_map = new HashMap<Integer, IMapState>();
			
			ArrayList<IMapState> maps = new ArrayList<IMapState>();
			for (IStateFactory sf : state_facts) {
				maps.add((IMapState) sf.makeState(context));
			}

			for (Entry<Integer, Integer> ent : opts_map.entrySet()) {
				idx_map.put(ent.getKey(), maps.get(ent.getValue()));
			}
			
			return new MultiState(def_state, idx_map, maps, bins_in);
		}
		
		@Override
		public Collection<? extends String> getUDFs() {
			List<String> udfs = new ArrayList<String>();
			
			// Add the wrapped UDFs
			for (IStateFactory sf : state_facts) {
				udfs.add(sf.getClass().getName());
				
				// Recurse in if necessary.
				if (IUDFExposer.class.isInstance(sf)) {
					IUDFExposer ex = (IUDFExposer) sf;
					udfs.addAll(ex.getUDFs());
				}
			}
			
			return udfs;
		}

	}

	@Override
	public List<IPigIdxState> multiGet(List<List<Object>> keys) {
		// Call gets on all the states and merge the results together.
		List<IPigIdxState> ret;
		
		if (def_state != null) {
			ret = def_state.multiGet(keys);
		} else {
			ret = new ArrayList<IPigIdxState>();
			for (int i = 0; i < keys.size(); i++) {
				ret.add(null);
			}
		}
		
		// Cycle through the maps and merge things.
		for (IMapState m : maps) {
			List<IPigIdxState> cur = m.multiGet(keys);
			for (int i = 0; i < cur.size(); i++) {
				if (ret.get(i) == null) {
					ret.set(i, cur.get(i));
				} else if (cur.get(i) != null) {
					ret.get(i).merge(cur.get(i));
				}
			}
		}

		return ret;
	}

	@Override
	public void multiPut(List<List<Object>> keys, List<IPigIdxState> vals) {
		// Yuck.
		ArrayList<List<Object>> def_pair_k = new ArrayList<List<Object>>();
		ArrayList<Writable> def_pair_v = new ArrayList<Writable>();
		ArrayList<IMapState> other_state = new ArrayList<IMapState>();
		ArrayList<List<Object>> other_keys = new ArrayList<List<Object>>();
		ArrayList<Writable> other_vals = new ArrayList<Writable>();
		
		for (int i = 0; i < keys.size(); i++) {
			Pair<Writable, List<Writable>> sep = vals.get(i).separate(bins);
			if (sep.first != null) {
				def_pair_k.add(keys.get(i));
				def_pair_v.add(sep.first);
			}
			for (int j = 0; j < bins.size(); j++) {
				if (sep.second.get(j) != null) {
					other_state.add(this.idx_map.get(bins.get(j)[0]));
					other_keys.add(keys.get(i));
					other_vals.add(sep.second.get(j));
				}
			}
		}
		
		// Call multiPut on the wrapped states.
		if (def_pair_k.size() > 0) {
			def_state.multiPut(def_pair_k, def_pair_v);
		}
		for (int i = 0; i < other_state.size(); i++) {
			other_state.get(i).multiPut(other_keys, other_vals);
		}
	}

	@Override
	public void commit(long txid) {
		// TODO Auto-generated method stub
		
	}
}
