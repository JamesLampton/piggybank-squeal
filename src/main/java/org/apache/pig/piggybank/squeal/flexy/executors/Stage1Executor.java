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

package org.apache.pig.piggybank.squeal.flexy.executors;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.io.Writable;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalCause;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Values;
import storm.trident.operation.CombinerAggregator;
import storm.trident.operation.TridentCollector;
import storm.trident.state.State;
import storm.trident.state.StateFactory;
import storm.trident.state.map.MapState;
import storm.trident.tuple.TridentTuple;

public class Stage1Executor<T> implements RemovalListener<Writable, T> {
	
	private LoadingCache<Writable, T> cache;
	private CombinerAggregator<T> agg;
	private TridentCollector collector;
	private int max_size = 1000;
	private int expiry_ms = 20;
	private StateFactory sf;
	Map<Writable, T> stateBacklog;
	List<List<Object>> prefetch;
	private MapState<T> state;
	private CombinerAggregator<T> storeAgg;
	
	public Stage1Executor(CombinerAggregator<T> agg, CombinerAggregator<T> storeAgg, StateFactory sf) {
		this.agg = agg;
		this.storeAgg = storeAgg;
		this.sf = sf;
	}

	public void setMaximumSize(int max_size) {
		this.max_size = max_size;
	}
	
	public void setExpiryMS(int value) {
		this.expiry_ms = value;
	}
	
	public void prepare(Map stormConf, TopologyContext context, 
			TridentCollector collector) {
		// Create the state.
		state = (MapState<T>) sf.makeState(stormConf, 
				null, 
				context.getThisTaskIndex(), 
				context.getComponentTasks(context.getThisComponentId()).size());
		
		// Create a state backlog.
		stateBacklog = new HashMap<Writable, T>();
		
		// Create the cache to hold the current computations before state manipulation.
		cache = CacheBuilder.newBuilder()
				.maximumSize(max_size)
				.expireAfterWrite(expiry_ms, TimeUnit.MILLISECONDS)
				.removalListener(this)
				.build(new CacheLoader<Writable, T>() {
					@Override
					public T load(Writable key) throws Exception {
						return agg.zero();
					}
				});
		
		this.collector = collector;
	}
	
	public void execute(Writable key, TridentTuple tuple) {
		try {
			// Pull the current value.
			T cur = cache.get(key);
			// Merge the new value.
			T next = agg.combine(cur, agg.init(tuple));
			// Replace the cached value.
			cache.put(key, next);
		} catch (ExecutionException e) {
			collector.reportError(e);
		}
		
	}
	
	public void flush() {
		cache.invalidateAll();
	}
	
	public void commit() {
		// Build lists of updates.
		List<List<Object>> keys = new ArrayList<List<Object>>(stateBacklog.size());
		List<T> vals = new ArrayList<T>(stateBacklog.size());
		
		for (Entry<Writable, T> ent : stateBacklog.entrySet()) {
			final Writable k = ent.getKey();
			keys.add(new ArrayList<Object>(1) {{ add(k); }});
			vals.add(ent.getValue());
		}
		
		// Push them out to the state.
		state.multiPut(keys, vals);
		
		// Clear the backlog.
		stateBacklog.clear();
	}
	
	public void runPrefetch() {
		List<T> fetched = state.multiGet(prefetch);
		for (int i = 0; i < prefetch.size(); i++) {
			T cur = fetched.get(i);
			if (cur == null) cur = storeAgg.zero();
			stateBacklog.put((Writable) prefetch.get(0).get(0), cur);
		}
		prefetch.clear();
	}

	@Override
	public void onRemoval(RemovalNotification<Writable, T> note) {
		if (!(note.wasEvicted() || note.getCause() == RemovalCause.EXPLICIT)) {
			return;
		}
		
		// Determine if the current value is in the backlog or the prefetch.
		T cur;
		if (!stateBacklog.containsKey(note.getKey())) {
			// Pull the values in from the prefetch.
			runPrefetch();
		}
		cur = stateBacklog.get(note.getKey());
		
		// Apply the update.
		cur = storeAgg.combine(cur, note.getValue());
		
		// Replace the backlog
		stateBacklog.put(note.getKey(), cur);
		
		// Emit the result.
		collector.emit(new Values(note.getKey(), cur));
	}
}
