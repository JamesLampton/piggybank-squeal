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
import storm.trident.state.StateFactory;
import storm.trident.state.map.MapState;
import storm.trident.tuple.TridentTuple;

public class Stage1Executor<T> implements RemovalListener<Writable, T> {
	public static final String CACHE_SIZE_CONF = "flexy.stage1.cache.size";
	public static final String CACHE_EXPIRY_CONF = "flexy.stage1.cache.expiry_ms";
	public static final String FLUSH_INTERVAL_CONF = "flexy.stage1.flush.interval";
	
	private LoadingCache<Writable, T> cache;
	private CombinerAggregator<T> agg;
	private TridentCollector collector;
	private int max_size = 1000;
	private int expiry_ms = 1000;
	
	private StateFactory sf;
	
	// This represents the current state value.
	Map<Writable, T> stateBacklog;
	// Values to pull from state.
	List<List<Object>> prefetch = new ArrayList<List<Object>>();
	// Expired values to be written.
	Map<Writable, T> writeAhead;
	
	private MapState<T> state;
	private CombinerAggregator<T> storeAgg;
	private Throwable lastThrown = null;
	private Writable activeKey;
	long last_flush = 0;
	long flush_interval_ms = -1;
	
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
		// And writeahead
		writeAhead = new HashMap<Writable, T>();
		
		// Pull configurations from conf.
		Number conf_int = (Number) stormConf.get(CACHE_SIZE_CONF);
		if (conf_int != null) max_size = conf_int.intValue(); 
		conf_int = (Number) stormConf.get(CACHE_EXPIRY_CONF);
		if (conf_int != null) expiry_ms= conf_int.intValue(); 
		conf_int = (Number) stormConf.get(FLUSH_INTERVAL_CONF);
		if (conf_int != null) flush_interval_ms = conf_int.intValue(); 
		
		// Create the cache to hold the current computations before state manipulation.
		cache = CacheBuilder.newBuilder()
				.maximumSize(max_size)
				.expireAfterWrite(expiry_ms, TimeUnit.MILLISECONDS)
				.removalListener(this)
				.build(new CacheLoader<Writable, T>() {
					@Override
					public T load(final Writable key) throws Exception {
						// If this value is in the writeAhead, save it
						if (writeAhead.containsKey(key)) {
							stateBacklog.put(key, writeAhead.remove(key));
						}
						// Determine if we've fetched the current state.
						if (!stateBacklog.containsKey(key)) {
							// Add to prefetch to pull the current data from store.
							prefetch.add(new ArrayList<Object>() {{ add(key); }} );
						}
						return agg.zero();
					}
				});
		
		this.collector = collector;
	}
	
	public void execute(final Writable key, TridentTuple tuple) {
		try {
			// Pull the current value.
			T cur = cache.get(key);
			activeKey = key;
			
			// Merge the new value.
			T next = agg.combine(cur, agg.init(tuple));
			
			// Replace the cached value.
			cache.put(key, next);
			if (lastThrown != null) {
				try {
					collector.reportError(lastThrown);
				} finally {
					lastThrown = null;
				}
			}
		} catch (Exception e) {
			collector.reportError(e);
		} finally {
			activeKey = null;
		}
		
	}
	
	public void flush() {
		if (flush_interval_ms > 0) {
			// Flush ever so often.
			long now = System.currentTimeMillis();
			if (now < flush_interval_ms + last_flush) {
				return;
			}
			last_flush = now;
		} else if (flush_interval_ms == -1) {
			// Never flush, but do allow for expiration.
			cache.cleanUp();
			return;
		}
		
		cache.invalidateAll();
		if (lastThrown != null) {
			try {
				collector.reportError(lastThrown);
			} finally {
				lastThrown = null;
			}
		}
	}
	
	public void commit(long txid) {
		// Build lists of updates.
		List<List<Object>> keys = new ArrayList<List<Object>>(stateBacklog.size());
		List<T> vals = new ArrayList<T>(stateBacklog.size());
		
		for (Entry<Writable, T> ent : writeAhead.entrySet()) {
			final Writable k = ent.getKey();
			keys.add(new ArrayList<Object>(1) {{ add(k); }});
			vals.add(ent.getValue());
		}
		
		// Push them out to the state.
		state.multiPut(keys, vals);
		state.commit(txid);
		
		// Clear the writeahead.
		writeAhead.clear();
	}
	
	public void runPrefetch() {
		List<T> fetched = state.multiGet(prefetch);
		for (int i = 0; i < prefetch.size(); i++) {
			T cur = fetched.get(i);
			if (cur == null) cur = storeAgg.zero();
//			System.err.println("Prefetched: " + prefetch.get(i).get(0) + " -- " + cur);
			stateBacklog.put((Writable) prefetch.get(i).get(0), cur);
		}
		prefetch.clear();
	}

	@Override
	public void onRemoval(RemovalNotification<Writable, T> note) {
//		System.err.println("S1 Cache: " + activeKey + " " + note.getKey() + " " + note.getCause() + " " + cache.size());
		if (!(note.wasEvicted() || note.getCause() == RemovalCause.EXPLICIT)) {
			return;
		}
		
		if (activeKey != null && activeKey.equals(note.getKey())) {
			return;
		}

		try {
			// Determine if the current value is in the backlog or the prefetch.
			T cur;
			if (!stateBacklog.containsKey(note.getKey())) {
				// Pull the values in from the prefetch.
				runPrefetch();
			}
			cur = stateBacklog.remove(note.getKey());
//			System.err.println("stateBacklogged: k=<" + note.getKey() + "> v=" + cur);

			// Apply the update.
			cur = storeAgg.combine(cur, note.getValue());

			// Move things to the writeAhead.
			writeAhead.put(note.getKey(), cur);

			// Emit the result.
			collector.emit(new Values(note.getKey(), cur));
		} catch (Throwable e) {
			lastThrown = e;
		}
	}
}
