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

package org.apache.pig.piggybank.squeal.backend.storm.state;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.pig.piggybank.squeal.flexy.oper.FlexyBaseFunction;
import org.apache.pig.piggybank.squeal.metrics.IMetricsTransport;
import org.apache.pig.piggybank.squeal.metrics.MetricsTransportFactory;

import storm.trident.state.map.IBackingMap;
import storm.trident.util.LRUMap;

// Hacked from: storm.trident.state.map.CacheMap v0.9.2
public class MetricsAwareCacheMap<T> implements IBackingMap<T> {
	LRUMap<List<Object>, T> _cache;
    IBackingMap<T> _delegate;
	private IMetricsTransport mt;
	boolean inited = false;

    public MetricsAwareCacheMap(IBackingMap<T> delegate, int cacheSize, Map conf) {
        _cache = new LRUMap<List<Object>, T>(cacheSize);
        _delegate = delegate;
        mt = MetricsTransportFactory.getInstance(conf, ClassLoader.getSystemClassLoader());
        if (mt != null) {
        	System.out.println("Initialized: MetricsAwareCacheMap " + _delegate + " " + mt + " " + mt.getSampleRate());
        }
    }

    void send(Object... msg) {
		StringBuilder sb = new StringBuilder();

		sb.append(System.currentTimeMillis());

		for (Object o : msg) {
			sb.append("\t");
			sb.append(o == null ? "" : o.toString().replace("\n", " ").replace("\t", " "));
		}
		sb.append("\n");

		mt.send(sb.toString().getBytes());
	}
    
    @Override
    public List<T> multiGet(List<List<Object>> keys) {
    	boolean collect_metrics = false;
    	long start_ts = 0;
    	int hit_count = 0;
    	int miss_count = 0;
    	int null_count = 0;
    	
    	if (mt != null && mt.shouldSample()) {
    		collect_metrics = true;
    		start_ts = System.nanoTime();
		}
    	
        Map<List<Object>, T> results = new HashMap<List<Object>, T>();
        List<List<Object>> toGet = new ArrayList<List<Object>>();
        for(List<Object> key: keys) {
            if(_cache.containsKey(key)) {
            	hit_count += 1;
                results.put(key, _cache.get(key));
            } else {
                toGet.add(key);
            }
        }

        List<T> fetchedVals = _delegate.multiGet(toGet);
        for(int i=0; i<toGet.size(); i++) {
            List<Object> key = toGet.get(i);
            T val = fetchedVals.get(i);
            if (val == null) {
            	null_count += 1;
            } else {
            	miss_count += 1;
            }
            _cache.put(key, val);
            results.put(key, val);
        }

        List<T> ret = new ArrayList<T>(keys.size());
        for(List<Object> key: keys) {
            ret.add(results.get(key));
        }
        
        if (collect_metrics) {
        	long stop_ts = System.nanoTime();
        	send("MULTI_GET", stop_ts - start_ts, hit_count, miss_count, null_count, _delegate.toString() + "\n");
        }
        
        return ret;
    }

    @Override
    public void multiPut(List<List<Object>> keys, List<T> values) {
    	boolean collect_metrics = false;
    	long start_ts = 0;
    	
    	if (mt != null && mt.shouldSample()) {
    		collect_metrics = true;
    		start_ts = System.nanoTime();
		}
    	
        cache(keys, values);
        _delegate.multiPut(keys, values);
        
        if (collect_metrics) {
        	long stop_ts = System.nanoTime();
        	send("MULTI_PUT", stop_ts - start_ts, keys.size(), _delegate.toString() + "\n");
        }
    }

    private void cache(List<List<Object>> keys, List<T> values) {
        for(int i=0; i<keys.size(); i++) {
            _cache.put(keys.get(i), values.get(i));
        }
    }
}
