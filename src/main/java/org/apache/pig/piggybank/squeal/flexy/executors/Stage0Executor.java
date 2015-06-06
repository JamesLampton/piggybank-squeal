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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
import storm.trident.tuple.TridentTuple;

public class Stage0Executor<T> implements RemovalListener<Writable, T> {
	private LoadingCache<Writable, T> cache;
	private CombinerAggregator<T> agg;
	private TridentCollector collector;
	private int max_size = 1000;
	private int expiry_ms = 20;
	private static final Log log = LogFactory.getLog(Stage0Executor.class);
	
	public Stage0Executor(CombinerAggregator<T> agg) {
		this.agg = agg;
	}

	public void setMaximumSize(int max_size) {
		this.max_size = max_size;
	}
	
	public void setExpiryMS(int value) {
		this.expiry_ms = value;
	}
	
	public void prepare(Map stormConf, TopologyContext context, 
			TridentCollector collector) {
		
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

	@Override
	public void onRemoval(RemovalNotification<Writable, T> note) {
		if (!(note.wasEvicted() || note.getCause() == RemovalCause.EXPLICIT)) {
			return;
		}

		// Emit the record.
		collector.emit(new Values(note.getKey(), note.getValue()));
	}
}
