package org.apache.pig.backend.storm.oper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.pig.backend.storm.oper.TriWindowCombinePersist.WindowCombineState;
import org.apache.pig.backend.storm.state.CombineTupleWritable;
import org.apache.pig.backend.storm.state.IPigIdxState;
import org.apache.pig.backend.storm.state.MapIdxWritable;
import org.apache.pig.impl.io.NullableTuple;
import org.apache.pig.impl.util.Pair;

import storm.trident.operation.CombinerAggregator;
import storm.trident.tuple.TridentTuple;

public class TriBasicPersist implements CombinerAggregator<MapIdxWritable> {
	
	static public List<NullableTuple> getTuples(MapIdxWritable state) {
		List<NullableTuple> ret = new ArrayList<NullableTuple>();
		
		for (Entry<Writable, Writable> ent : state.entrySet()) {
			int c = ((IntWritable) ent.getValue()).get();
			NullableTuple v = (NullableTuple) ent.getKey();
			// If c is negative then we may have seen the inverse tuple for 
			// a positive tuple we've yet to see.  This will silently consume
			// them.
			// FIXME: Or we have a terrible problem...
			for (int i = 0; i < c; i++) {
				ret.add(v);
			}
		}
		
		return ret;
	}

	@Override
	public MapIdxWritable init(TridentTuple tuple) {
		MapIdxWritable ret = zero();
		NullableTuple values = (NullableTuple) tuple.get(1);
		
		// Track the +/- stuff through.
		ret.put(values, new IntWritable(tuple.getInteger(2)));
		return ret;
	}

	@Override
	public MapIdxWritable combine(MapIdxWritable val1, MapIdxWritable val2) {
		MapIdxWritable ret = zero();
		
		if (val1 != null) {
			for (Entry<Writable, Writable> ent : val1.entrySet()) {
				ret.put(ent.getKey(), new IntWritable(((IntWritable) ent.getValue()).get()));
			}
		}
		
		// We're going to merge into val1.
		if (val2 != null) {
			for (Entry<Writable, Writable> ent : val2.entrySet()) {
				int c = ((IntWritable) ent.getValue()).get();
				IntWritable iw = (IntWritable) ret.get(ent.getKey());
				if (iw == null) {
					iw = new IntWritable(c);
					ret.put(ent.getKey(), iw);
				} else {
					iw.set(iw.get() + c);
				}
			}
		}
		
		return ret;
	}
	
	public static class TriBasicPersistState extends MapIdxWritable<TriBasicPersistState> {
		@Override
		public Pair<Writable, List<Writable>> separate(List<Integer[]> bins) {
			MapIdxWritable def = null; // = new TriBasicPersistState();
			List<MapIdxWritable> ret = new ArrayList<MapIdxWritable>(bins.size());
			HashMap<Integer, Integer> idxMap = new HashMap<Integer, Integer>();
			
			for (Integer[] bin : bins) {
//				MapIdxWritable st = new TriBasicPersistState();
				
				for (Integer i : bin) {
					idxMap.put(i, ret.size());
				}
				ret.add(null);
			}
			
			for (Entry<Writable, Writable> ent : entrySet()) {
				NullableTuple v = (NullableTuple) ent.getKey();
				int idx = v.getIndex();

				// Look up the appropriate state.
				Integer mappedIdx = idxMap.get(idx);
				MapIdxWritable mapped;
				if (mappedIdx == null) {
					if (def == null) {
						def = new TriBasicPersistState();
					}
					mapped = def;
				} else {
					mapped = ret.get(mappedIdx);
					if (mapped == null) {
						mapped = new TriBasicPersistState();
						ret.set(mappedIdx, mapped);
					}
				}
				mapped.put(ent.getKey(), ent.getValue());
			}
			
			return new Pair(def, ret);
		}

		@Override
		public void merge(IPigIdxState other) {
			putAll(((TriBasicPersistState)other));
		}

		@Override
		public List<NullableTuple> getTuples(Text which) {
			return TriBasicPersist.getTuples(this);
		}
		
		@Override
		public List<Pair<List<NullableTuple>, List<NullableTuple>>> getTupleBatches(
				TriBasicPersistState lastState) {
		
			List<NullableTuple> first = null;
			if (lastState != null) {
				first = lastState.getTuples(null);
			}
			List<NullableTuple> second = getTuples(null);

			Pair<List<NullableTuple>, List<NullableTuple>> p = 
					new Pair<List<NullableTuple>, List<NullableTuple>>(first, second);

			ArrayList<Pair<List<NullableTuple>, List<NullableTuple>>> ret = 
					new ArrayList<Pair<List<NullableTuple>, List<NullableTuple>>>();
			ret.add(p);
			return ret;			
		}
	}

	@Override
	public MapIdxWritable zero() {
		return new TriBasicPersistState();
	}
}
