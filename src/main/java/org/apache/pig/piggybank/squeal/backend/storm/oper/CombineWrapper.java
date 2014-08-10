package org.apache.pig.backend.storm.oper;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.pig.backend.storm.state.IPigIdxState;
import org.apache.pig.backend.storm.state.MapIdxWritable;
import org.apache.pig.impl.io.NullableTuple;
import org.apache.pig.impl.util.Pair;

import storm.trident.operation.CombinerAggregator;
import storm.trident.tuple.TridentTuple;

public class CombineWrapper implements CombinerAggregator<MapIdxWritable> {
	private CombinerAggregator<Writable> agg;
	private boolean trackLast;
	static public final Text CUR = new Text("cur");
	static public final Text LAST = new Text("last");
	
	public CombineWrapper(CombinerAggregator agg, boolean trackLast) {
		this.agg = (CombinerAggregator<Writable>) agg;
		this.trackLast = trackLast;
	}

	public CombineWrapper(CombinerAggregator agg) {
		this(agg, false);
	}

	@Override
	public MapIdxWritable init(TridentTuple tuple) {
		MapIdxWritable ret = zero();
		ret.put(CUR, agg.init(tuple));
		return ret;
	}

	Writable getDefault(MapIdxWritable v, Text k) {
		if (v.containsKey(k)) {
			return v.get(k);
		}
		return agg.zero();
	}
	
	@Override
	public MapIdxWritable combine(MapIdxWritable val1, MapIdxWritable val2) {
//		System.out.println("combine: " + val1 + " " + val2);
		MapIdxWritable ret = zero();

		// Assuming that val1 came from the cache/state.
		if (trackLast && val1.get(CUR) != null) {
			ret.put(LAST, val1.get(CUR));
		}
		ret.put(CUR, agg.combine(getDefault(val1, CUR), getDefault(val2, CUR)));
			
		return ret;
	}
	
	public static class CombineWrapperState extends MapIdxWritable<CombineWrapperState> {
		@Override
		public List<NullableTuple> getTuples(Text which) {
			return CombineWrapper.getTuples(this, which);
		}
		
		@Override
		public List<Pair<List<NullableTuple>, List<NullableTuple>>> getTupleBatches(
				CombineWrapperState last) {
			// Last should be null from TriReduce...
			return CombineWrapper.getTupleBatches(this);
		}
		
		@Override
		public Pair<Writable, List<Writable>> separate(List<Integer[]> bins) {
			MapIdxWritable def = null;
			List<MapIdxWritable> ret = new ArrayList<MapIdxWritable>(bins.size());
			
			for (Integer[] bin : bins) {
				ret.add(null);
			}
			
			for (Entry<Writable, Writable> ent : entrySet()) {
				IPigIdxState wrapped = (IPigIdxState) ent.getValue();
				
				Pair<Writable, List<Writable>> res = wrapped.separate(bins);
				// Set the appropriate bins in this return.
				if (res.first != null) {
					if (def == null) {
						def = new CombineWrapperState();
					}
					def.put(ent.getKey(), res.first);					
				}

				for(int i = 0; i < res.second.size(); i++) {
					if (res.second.get(i) != null) {
						if (ret.get(i) == null) {
							ret.set(i, new CombineWrapperState());
						}
						ret.get(i).put(ent.getKey(), res.second.get(i));
					}
				}
			}
			
			return new Pair(def, ret);
		}

		@Override
		public void merge(IPigIdxState other) {
			for (Entry<Writable, Writable> ent : ((CombineWrapperState)other).entrySet()) {
				IPigIdxState cur = (IPigIdxState) get(ent.getKey());
				if (cur == null) {
					put(ent.getKey(), ent.getValue());
				} else {
					cur.merge((IPigIdxState) ent.getValue());
				}
			}
		}
	}

	@Override
	public MapIdxWritable zero() {
		return new CombineWrapperState();
	}
	
	public static Comparator<NullableTuple> NullableTupleComparator = new Comparator<NullableTuple>() {
		@Override
		public int compare(NullableTuple o1, NullableTuple o2) {
			int res = o1.getIndex() - o2.getIndex();
			if (res == 0) {
				return o1.compareTo(o2);
			}
			return res;
		}
	};

	public static List<NullableTuple> getTuples(MapIdxWritable m, Text which) {

		IPigIdxState state = (IPigIdxState) m.get(which);
		if (state == null) {
			return null;
		}
		List<NullableTuple> ret = state.getTuples(which);		
		// Sort the tuples as the shuffle would.
		Collections.sort(ret, NullableTupleComparator);
		
		return ret;
	}
	
	public static List<Pair<List<NullableTuple>, List<NullableTuple>>> getTupleBatches(
			CombineWrapperState cws) {
		
		IPigIdxState state = (IPigIdxState) cws.get(CUR);
		if (state == null) {
			throw new RuntimeException("No current state in CombineWrapperState: " + cws);
		}

		return state.getTupleBatches((IPigIdxState) cws.get(LAST));
	}
}
