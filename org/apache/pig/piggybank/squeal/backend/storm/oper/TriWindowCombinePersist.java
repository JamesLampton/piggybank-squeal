package org.apache.pig.backend.storm.oper;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.pig.backend.storm.state.IPigIdxState;
import org.apache.pig.backend.storm.state.MapIdxWritable;
import org.apache.pig.backend.storm.state.WindowBundle;
import org.apache.pig.impl.io.NullableTuple;
import org.apache.pig.impl.util.Pair;
import org.mortbay.util.ajax.JSON;

import storm.trident.operation.CombinerAggregator;
import storm.trident.tuple.TridentTuple;

public class TriWindowCombinePersist implements CombinerAggregator<MapIdxWritable> {
	
	Map<Integer, Long> windowSettings = new HashMap<Integer, Long>();
	
	public TriWindowCombinePersist() {
		this(null);
	}
	
	public TriWindowCombinePersist(String windowOptions) {
		// Parse the options if they're not null.
		if (windowOptions != null) {
			Map<String, Long> opts = (Map<String, Long>) JSON.parse(windowOptions);
			for (Entry<String, Long> ent : opts.entrySet()) {
				int i = Integer.parseInt(ent.getKey());
				windowSettings.put(i, ent.getValue());
			}
		}
	}
	
	static Pair<List<NullableTuple>, WindowBundle<NullableTuple>> extractStates(WindowCombineState state) {
		if (state == null) {
			return new Pair<List<NullableTuple>, WindowBundle<NullableTuple>>(null, null);
		}
		
		List<NullableTuple> fixed = new ArrayList<NullableTuple>();
		WindowBundle<NullableTuple> win = null;
		
		for (Entry<Writable, Writable> ent : state.entrySet()) {
			if (ent.getKey().getClass().isAssignableFrom(IntWritable.class)) {
				// This is a windowed element.
				if (win != null) {
					// If we have multiple windows on a particular node, I'm not sure how we would
					// sequence things together given the current implementation.
					throw new RuntimeException("ERROR: Only one windowed element per group is currently supported.");
				}
				
				win = (WindowBundle<NullableTuple>) ent.getValue();
			} else {
				// This is a counted element.
				int c = ((IntWritable) ent.getValue()).get();
				NullableTuple v = (NullableTuple) ent.getKey();
				for (int i = 0; i < c; i++) {
					fixed.add(v);
				}
			}
		}
		
		return new Pair<List<NullableTuple>, WindowBundle<NullableTuple>>(fixed, win);
	}

	static List<NullableTuple> mergeOrCopy(List<NullableTuple> to, List<NullableTuple> from) {
		if (to == null && from == null) {
			return null;
		}
		
		if (to == null && from != null) {
			return new ArrayList<NullableTuple>(from);
		}
		
		// to isn't null.
		if (from != null) {
			to.addAll(from);
		}
		
		return to;
	}
	
	public static List<Pair<List<NullableTuple>, List<NullableTuple>>> getTupleBatches(
			WindowCombineState curState, WindowCombineState lastState) {
		// First, separate the windowed portions.
		Pair<List<NullableTuple>, WindowBundle<NullableTuple>> lastStuff = extractStates(lastState);
		Pair<List<NullableTuple>, WindowBundle<NullableTuple>> curStuff = extractStates(curState);
		
//		System.out.println("getTupleBatches: last: " + lastStuff.first + " " + lastStuff.second);
//		System.out.println("getTupleBatches: cur: " + curStuff.first + " " + curStuff.second);
		
		// Now build the pairs.
		List<Pair<List<NullableTuple>, List<NullableTuple>>> arr = 
				WindowBundle.getTuples(lastStuff.second, curStuff.second); 
		for (Pair<List<NullableTuple>, List<NullableTuple>> p_win : arr) {
//			System.out.println("1 getTupleBatches pair first: " + p_win.first + " ---second: " + p_win.second);
			
			// Expand each list by the static portion.
			p_win.first = mergeOrCopy(p_win.first, lastStuff.first);
			p_win.second = mergeOrCopy(p_win.second, curStuff.first);
			
//			System.out.println("2 getTupleBatches pair first: " + p_win.first + " ---second: " + p_win.second);
		}
		
		return arr;
	}
	
	void addTuple(MapIdxWritable s, NullableTuple t, int c) {
		int idx = t.getIndex();
		Long ws = windowSettings.get(idx);
		if (ws != null) {
			IntWritable key_tmp = new IntWritable(idx);

			// Pull the window.
			WindowBundle<NullableTuple> w = 
					(WindowBundle<NullableTuple>) s.get(key_tmp);
			
			/*
			 * FIXME: If we get the negative before the positive, this won't work.
			 * The proper way to do this would be to count the removes in window
			 * state so we can ignore adds when the matching positive values come
			 * in. 
			 */
			if (c < 0) {
				// Remove the item for negative items.
				w.remove(t);
			} else {
				// Add it otherwise.
				w.push(t);
			}
		} else {
			// This is not a windowed element, just add like BASEPERSIST.
			IntWritable iw = (IntWritable) s.get(t);
			if (iw == null) {
				iw = new IntWritable(c);
				s.put(t, iw);
			} else {
				iw.set(iw.get() + c);
			}
		}
	}
	
	@Override
	public MapIdxWritable init(TridentTuple tuple) {
		MapIdxWritable ret = zero();
		NullableTuple values = (NullableTuple) tuple.get(1);
		
		// Track the +/- stuff through.
		addTuple(ret, values, tuple.getInteger(2));
		return ret;
	}
	
	String dumpToString(MapIdxWritable state) {
		StringBuilder sb = new StringBuilder();
		
		sb.append(this.getClass().getName()+"[");
		
		for (Entry<Writable, Writable> ent : state.entrySet()) {
			if (ent.getKey().getClass().isAssignableFrom(NullableTuple.class)) {
				// It's a tuple -> count.
				sb.append(ent.getKey().toString());
				sb.append("->" + ent.getValue());
			} else {
				// It's a window.
				sb.append(ent.getKey().toString());
				sb.append("(idx)->" + ent.getValue());
			}
			
			sb.append(", ");
		}
		
		sb.append("]");
		
		return sb.toString();
	}
	
	void mergeValues(MapIdxWritable into, MapIdxWritable from) {
		for (Entry<Writable, Writable> ent : from.entrySet()) {
			// See if this is a windowed element.
			if (ent.getKey() instanceof IntWritable) {
				// Pull the window.
				WindowBundle<NullableTuple> w = 
						(WindowBundle<NullableTuple>) into.get(ent.getKey());
				
				// Merge win2 in to w.
				WindowBundle<NullableTuple> w2 = (WindowBundle<NullableTuple>) ent.getValue();
				w.merge(w2);
				
				// Remove any aged off windows.
				w.runAgeoff(System.currentTimeMillis());
				
				continue;
			}
			
			// Otherwise it's a tuple, merge it.
			NullableTuple v = (NullableTuple) ent.getKey();
			int c = ((IntWritable)ent.getValue()).get();
			this.addTuple(into, v, c);
		}
	}

	@Override
	public MapIdxWritable combine(MapIdxWritable val1, MapIdxWritable val2) {
//		System.out.println("TriWindowCombinePersist.combine -- ");
//		System.out.println("val1 -- " + dumpToString(val1));
//		System.out.println("val2 -- " + dumpToString(val2));
		
		MapIdxWritable ret = zero();
		
		// Merge the values
		mergeValues(ret, val1);
		mergeValues(ret, val2);
		
		return ret;
	}
	
	static public class WindowCombineState extends MapIdxWritable<WindowCombineState> {
		private Map<Integer, Long> settings;
		
		// Override default write to track the settings in case
		// some of the elements age off.
		@Override
		public void write(DataOutput out) throws IOException {
			out.writeInt(settings.size());
			for (java.util.Map.Entry<Integer, Long> ent : settings.entrySet()) {
				out.writeInt(ent.getKey());
				out.writeLong(ent.getValue());
			}
			super.write(out);
		}
		
		@Override
		public void readFields(DataInput in) throws IOException {
			int size = in.readInt();
			this.clear();
			Map<Integer, Long> in_set = new HashMap<Integer, Long>(size);
			for (int i = 0; i < size; i++) {
				int k = in.readInt();
				long v = in.readLong();
				in_set.put(k, v);
			}
			init(in_set);
			super.readFields(in);
		}

		public WindowCombineState() {
			// Needed for readFields.
		}
		
		public WindowCombineState(Map<Integer, Long> windowSettings) {
			init(windowSettings);
		}
		
		void init(Map<Integer, Long> windowSettings) {
			this.settings = windowSettings;
			
			// Initialize any windows.
			for (Entry<Integer, Long> ent : windowSettings.entrySet()) {
				put(new IntWritable(ent.getKey()), 
						new WindowBundle<NullableTuple>(
								ent.getValue().intValue()));
			}
		}

		@Override
		public Pair<Writable, List<Writable>> separate(List<Integer[]> bins) {
			MapIdxWritable def = null;
			List<MapIdxWritable> ret = new ArrayList<MapIdxWritable>(bins.size());
			HashMap<Integer, Integer> idxMap = new HashMap<Integer, Integer>();
			
			for (Integer[] bin : bins) {
//				MapIdxWritable st = new WindowCombineState(settings);
				for (Integer i : bin) {
					idxMap.put(i, ret.size());
				}
				ret.add(null);
			}
			
			for (Entry<Writable, Writable> ent : entrySet()) {
				int idx;
				if (ent.getKey().getClass().isAssignableFrom(IntWritable.class)) {
					// This is a windowed element.
					idx = ((IntWritable)ent.getKey()).get(); 
				} else {
					NullableTuple v = (NullableTuple) ent.getKey();
					idx = v.getIndex();
				}

				// Look up the appropriate state.
				Integer mappedIdx = idxMap.get(idx);
				MapIdxWritable mapped; // = idxMap.get(idx);
				if (mappedIdx == null) {
					if (def == null) {
						def = new WindowCombineState(settings);
					}
					mapped = def;
				} else {
					mapped = ret.get(mappedIdx);
					if (mapped == null) {
						mapped = new WindowCombineState(settings);
						ret.set(mappedIdx, mapped);
					}
				}
				mapped.put(ent.getKey(), ent.getValue());
			}
			
			return new Pair(def, ret);
		}

		@Override
		public void merge(IPigIdxState other) {
			for (Entry<Writable, Writable> ent : ((WindowCombineState)other).entrySet()) {
				if (ent.getKey().getClass().isAssignableFrom(IntWritable.class)) {
					// This is a windowed element.
					WindowBundle<NullableTuple> w = (WindowBundle<NullableTuple>) ent.getValue();
					// The windows should have been partitioned, so if we have anything within
					// replace ours.
					if (!w.isEmpty()) {
						put(ent.getKey(), ent.getValue());
					}
				} else {
					// Not windowed, just put it.
					put(ent.getKey(), ent.getValue());
				}
			}
		}

		@Override
		public List<NullableTuple> getTuples(Text which) {
			throw new RuntimeException("Not implemented for " + this.getClass().getName());
		}
		
		@Override
		public List<Pair<List<NullableTuple>, List<NullableTuple>>> getTupleBatches(
				WindowCombineState lastState) {
			return TriWindowCombinePersist.getTupleBatches(this, lastState);
		}
	}
	
	@Override
	public MapIdxWritable zero() {
		MapIdxWritable ret = new WindowCombineState(windowSettings);
		
		return ret;
	}
}
