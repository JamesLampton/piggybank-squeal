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

import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Writable;
import org.apache.pig.piggybank.squeal.flexy.components.ICollector;
import org.apache.pig.piggybank.squeal.flexy.components.ICombinerAggregator;
import org.apache.pig.piggybank.squeal.flexy.components.IFlexyTuple;
import org.apache.pig.piggybank.squeal.flexy.components.IRunContext;
import org.apache.pig.piggybank.squeal.flexy.model.FValues;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalCause;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;

public class Stage0Executor<T> implements RemovalListener<Writable, T> {
	public static final String CACHE_SIZE_CONF = "flexy.stage0.cache.size";
	public static final String CACHE_EXPIRY_CONF = "flexy.stage0.cache.expiry_ms";
	public static final String FLUSH_INTERVAL_CONF = "flexy.stage0.flush.interval";
	public static final String CACHE_STATS_INTERVAL_CONF = "flexy.stage0.cache.stats.interval_min";

	private LoadingCache<Writable, T> cache;
	private ICombinerAggregator<T> agg;
	private ICollector collector;
	private int max_size = 1000;
	private int expiry_ms = 1000;
	private static final Log log = LogFactory.getLog(Stage0Executor.class);
	Throwable lastThrown = null;
	Writable activeKey = null;
	long last_flush = 0;
	long flush_interval_ms = 0;
	
	long last_stats_dump = 0;
	long cache_stats_interval_min = 0;
	
	public Stage0Executor(ICombinerAggregator<T> agg) {
		this.agg = agg;
	}

	public void setMaximumSize(int max_size) {
		this.max_size = max_size;
	}
	
	public void setExpiryMS(int value) {
		this.expiry_ms = value;
	}
	
	public void prepare(IRunContext context, ICollector collector) {
		
		// Pull configurations from conf.
		Number conf_int = (Number) context.get(CACHE_SIZE_CONF);
		if (conf_int != null) max_size = conf_int.intValue(); 
		conf_int = (Number) context.get(CACHE_EXPIRY_CONF);
		if (conf_int != null) expiry_ms= conf_int.intValue(); 
		conf_int = (Number) context.get(FLUSH_INTERVAL_CONF);
		if (conf_int != null) flush_interval_ms = conf_int.intValue(); 
		conf_int = (Number) context.get(CACHE_STATS_INTERVAL_CONF);
		if (conf_int != null) cache_stats_interval_min = conf_int.intValue(); 
		
		CacheBuilder<Writable, T> cb = CacheBuilder.newBuilder()
				.maximumSize(max_size)
				.expireAfterWrite(expiry_ms, TimeUnit.MILLISECONDS)
				.removalListener(this);
		
		if (cache_stats_interval_min > 0) {
			try {
				cb = cb.recordStats();	
			} catch (NoSuchMethodError e) {
				// FIXME: Guava/assembly confusion...
			}
		}
				
		cache = cb
				.build(new CacheLoader<Writable, T>() {
					@Override
					public T load(Writable key) throws Exception {
						return agg.zero();
					}
				});
		
		this.collector = collector;
	}
	
	public void execute(Writable key, IFlexyTuple tuple) {
		try {
			// Pull the current value.
			T cur = cache.get(key);
			activeKey = key;
			
			// Merge the new value.
			T next = agg.combine(cur, agg.init(tuple));
			// Replace the cached value.
//			System.err.println("  s0exec: " + key + " cur -- " + cur + " next -- " + next);
			cache.put(key, next);
			checkError();
		} catch (Exception e) {
			collector.reportError(e);
		} finally {
			activeKey = null;
		}
	}
	
	void checkError() {
		if (lastThrown != null) {
			try {
				collector.reportError(lastThrown);
			} finally {
				lastThrown = null;
			}
		}
	}
	
	public void flush() {
		if (cache_stats_interval_min > 0) {
			long now = System.currentTimeMillis();
			if (now >= last_stats_dump + cache_stats_interval_min * 60000) {
				last_stats_dump = now;
				log.info("s0 cache stats: " + cache.stats());
			}
		}
		
//		if (cache.size() > 0) { System.err.println("		XXXX FLUSHING XXXX"); }
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
			checkError();
			return;
		}
		
		cache.invalidateAll();
		checkError();
	}

	@Override
	public void onRemoval(RemovalNotification<Writable, T> note) {
//		System.err.println("S0 Cache: " + activeKey + " " + note.getKey() + " " + note.getCause() + " " + cache.size());
		if (!(note.wasEvicted() || note.getCause() == RemovalCause.EXPLICIT)) {
			return;
		}
		if (activeKey != null && activeKey.equals(note.getKey())) {
			return;
		}

		try {
//			System.err.println("  s0emit: " + note.getCause() + " " + note.getKey() + " " + note.getValue());
			// Emit the record.
			collector.emit(new FValues(note.getKey(), note.getValue()));
		} catch (Throwable e) {
			lastThrown = e;
		}
	}
}
