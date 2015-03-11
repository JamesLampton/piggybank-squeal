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

package org.apache.pig.piggybank.squeal.backend.storm.oper;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.pig.piggybank.squeal.backend.storm.state.IPigIdxState;
import org.apache.pig.piggybank.squeal.backend.storm.state.MapIdxWritable;
import org.apache.pig.impl.io.NullableTuple;
import org.apache.pig.impl.util.Pair;

import storm.trident.operation.CombinerAggregator;
import storm.trident.tuple.TridentTuple;

public class CombineWrapper implements CombinerAggregator<MapIdxWritable> {
	private CombinerAggregator<Writable> agg;
	private boolean trackLast;
	private boolean identityInit;
	static public final Text CUR = new Text("cur");
	static public final Text LAST = new Text("last");
	
	public CombineWrapper(CombinerAggregator agg, boolean trackLast, boolean identityInit) {
		this.agg = (CombinerAggregator<Writable>) agg;
		this.trackLast = trackLast;
		this.identityInit = identityInit;
	}

	public CombineWrapper(CombinerAggregator agg, boolean trackLast) {
		this(agg, trackLast, false);
	}
	
	public CombineWrapper(CombinerAggregator agg) {
		this(agg, false);
	}

	@Override
	public MapIdxWritable init(TridentTuple tuple) {
//		System.out.println("init: " + tuple + " identityInit: " + identityInit + " agg: " + agg);
		if (identityInit) {
			return (MapIdxWritable) tuple.get(0);
		}
		
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
//		System.out.println("	combine: " + val1 + " " + val2);
		MapIdxWritable ret = zero();

		// Assuming that val1 came from the cache/state.
		if (trackLast && val1.get(CUR) != null) {
			ret.put(LAST, val1.get(CUR));
		}
		Writable combined = agg.combine(getDefault(val1, CUR), getDefault(val2, CUR));
		if (combined == null) {
			throw new RuntimeException("null combine: " + val1 + " " + val2);
		}
		ret.put(CUR, combined);
			
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

	public static List<NullableTuple> getTuples(MapIdxWritable m, Text which) {

		IPigIdxState state = (IPigIdxState) m.get(which);
		if (state == null) {
			return null;
		}
		List<NullableTuple> ret = state.getTuples(which);
		
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
	
	public static class Factory {
		private CombinerAggregator agg;

		public Factory(CombinerAggregator agg) {
			this.agg = agg;
		}
		
		public CombineWrapper getStage1Aggregator() {
			return new CombineWrapper(agg, false);
		}
		
		public CombineWrapper getStage2Aggregator() {
			return new CombineWrapper(agg, false, true);
		}
		
		public CombineWrapper getStoreAggregator() {
			return new CombineWrapper(agg, true);
		}
	}
}
