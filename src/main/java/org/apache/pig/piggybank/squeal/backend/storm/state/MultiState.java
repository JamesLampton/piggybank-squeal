package org.apache.pig.backend.storm.state;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.Writable;
import org.apache.pig.impl.util.Pair;

import backtype.storm.task.IMetricsContext;

import storm.trident.state.State;
import storm.trident.state.StateFactory;
import storm.trident.state.map.IBackingMap;
import storm.trident.state.map.MapState;
import storm.trident.state.map.NonTransactionalMap;

public class MultiState implements IBackingMap<IPigIdxState> {

	private MapState def_state;
	private Map<Integer, MapState> idx_map;
	private ArrayList<MapState> maps ;
	List<Integer[]> bins;

	private MultiState(MapState def_state, Map<Integer, MapState> idx_map, ArrayList<MapState> maps, ArrayList<Integer[]> bins_in) {
		this.def_state = def_state;
		this.idx_map = idx_map;
		this.bins = bins_in;
		this.maps = maps;
	}
	
	public static StateFactory fromJSONArgs(HashMap args) {
		return new Factory(args);
	}
	
	public static class Factory implements StateFactory {
		Map<Integer, Integer> opts_map = new HashMap<Integer, Integer>();
		StateFactory def_fact;
		private ArrayList<Integer[]> bins_in;
		private ArrayList<StateFactory> state_facts;
		static final Integer[] dummy = new Integer[0];
		
		public String toString() {
			return "MultiState(default: " + def_fact + ", " + state_facts + ", " + opts_map  + ")";
		}
		
		public Factory(HashMap args) {
//			System.out.println("MultiState.Factory: " + args);
			bins_in = new ArrayList<Integer[]>();
			state_facts = new ArrayList<StateFactory>();
			
			// Parse the args.
			for (Object k : args.keySet()) {
//				System.out.println("MultiState.Factory: " + k + " -> " + args.get(k));
				Map opts = (Map) args.get(k);
				StateFactory k_fact = StateWrapper.getStateFactoryFromArgs(
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
		public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
			MapState def_state = null;
			if (def_fact != null) {
				def_state = (MapState) def_fact.makeState(conf, metrics, partitionIndex, numPartitions);
			}
			
			Map<Integer, MapState> idx_map = new HashMap<Integer, MapState>();
			
			ArrayList<MapState> maps = new ArrayList<MapState>();
			for (StateFactory sf : state_facts) {
				maps.add((MapState) sf.makeState(conf, metrics, partitionIndex, numPartitions));
			}

			for (Entry<Integer, Integer> ent : opts_map.entrySet()) {
				idx_map.put(ent.getKey(), maps.get(ent.getValue()));
			}
			
			return NonTransactionalMap.build(new MultiState(def_state, idx_map, maps, bins_in));
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
		for (MapState m : maps) {
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
		ArrayList<MapState> other_state = new ArrayList<MapState>();
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

}
