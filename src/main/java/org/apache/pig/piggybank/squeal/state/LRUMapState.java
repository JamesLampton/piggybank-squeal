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
package org.apache.pig.piggybank.squeal.state;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

import org.apache.pig.piggybank.squeal.flexy.components.IMapState;
import org.apache.pig.piggybank.squeal.flexy.components.IRunContext;
import org.apache.pig.piggybank.squeal.flexy.components.IStateFactory;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

public class LRUMapState<T> implements IMapState<T> {

	public static class Factory implements IStateFactory {

		private int _cache_size;
		private String _id;

		public Factory(int cacheSize) {
			_id = UUID.randomUUID().toString();
			this._cache_size = cacheSize;
		}

		@Override
		public IMapState makeState(IRunContext context) {
			return new LRUMapState(_cache_size, _id + context.getPartitionIndex());
		}

	}
	
	LoadingCache<Object, Object> makeCache(int cacheSize) {
		LoadingCache<Object, Object> cache = CacheBuilder.newBuilder()
				.maximumSize(cacheSize)
				.build(
						new CacheLoader<Object, Object>() {
							public Object load(Object key) {
								// Guava will throw an exception.
								return null;
							}
						});
		return cache;
	}

	private LoadingCache<Object, Object> db;
	static ConcurrentHashMap<String, LoadingCache<Object, Object>> _dbs = new ConcurrentHashMap<String, LoadingCache<Object, Object>>();
	
	public LRUMapState(int cacheSize, String name) {
		if (!_dbs.containsKey(name)) {
            _dbs.put(name, makeCache(cacheSize));
        }
		this.db = (LoadingCache<Object, Object>) _dbs.get(name);
	}

	@Override
	public List<T> multiGet(List<List<Object>> keys) {
		List<T> ret = new ArrayList<T>();
        for (List<Object> key : keys) {
        	ret.add((T) db.getIfPresent(key));
        }
        return ret;
	}

	@Override
	public void multiPut(List<List<Object>> keys, List<T> vals) {
		for (int i = 0; i < keys.size(); i++) {
            List<Object> key = keys.get(i);
            T val = vals.get(i);
            db.put(key, val);
        }
	}

	@Override
	public void commit(long txid) {
		// TODO Auto-generated method stub
		
	}

}
