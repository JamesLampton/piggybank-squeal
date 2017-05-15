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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.pig.piggybank.squeal.backend.storm.state.MetricsAwareCacheMap;
import org.apache.pig.piggybank.squeal.flexy.components.IMapState;
import org.apache.pig.piggybank.squeal.flexy.components.IRunContext;
import org.apache.pig.piggybank.squeal.flexy.components.ISerializer;
import org.apache.pig.piggybank.squeal.flexy.components.IStateFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.ShardedJedis;

public class RedisState<T> implements IMapState<T> {
	
	/*
	 * Options for the Redis state.
	 *  - localCacheSize - Number of elements to hold in memory.
	 *  - serialize - Used for object marshalling.
	 *  - expiration - Expiration in seconds for elements.
	 *  - dbNum - database to select upon connect.
	 *  - sep - Compound key separater.
	 */
	public static class RedisOptions<T> implements Serializable {
        public int localCacheSize = 1000;
        public ISerializer<T> serializer = null;
        public ISerializer key_serializer = null;
        public int expiration = 0;
        public int dbNum = 0;
        public String sep = "|";        
    }
	
	public static IStateFactory fromJSONArgs(HashMap args) {
		// Create a default options:
		RedisOptions opts = new RedisOptions();
		// Pull the server list from the args.
		String servers = (String) args.get("servers");
		
		// Pull out non-default stuff.
		if (args.get("localCacheSize") != null) {
			opts.localCacheSize = Integer.parseInt(args.get("localCacheSize").toString());
		}
		if (args.get("serializer") != null) {
			String cn = (String) args.get("serializer");
			// Special case here -- pull the class name and set it up.
			try {
				Class<?> cls = Class.forName(cn);
				opts.serializer = (ISerializer) cls.newInstance();	
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
		if (args.get("key_serializer") != null) {
			String cn = (String) args.get("key_serializer");
			// Special case here -- pull the class name and set it up.
			try {
				Class<?> cls = Class.forName(cn);
				opts.key_serializer = (ISerializer) cls.newInstance();	
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
		if (args.get("expiration") != null) {
			opts.expiration = Integer.parseInt(args.get("expiration").toString());
		}
		if (args.get("dbNum") != null) {
			opts.dbNum = Integer.parseInt(args.get("dbNum").toString());
		}
		if (args.get("sep") != null) {
			opts.sep = (String) args.get("sep");
		}
		
		return new Factory(servers, opts);
	}

    /*
     * Factory for creating RedisStates.
     */
    protected static class Factory implements IStateFactory {
        String _servers;
        ISerializer _ser;
        RedisOptions _opts;

        public Factory(String servers, RedisOptions options) {
            _servers = servers;
            _opts = options;
            if(options.serializer==null) {
                throw new RuntimeException("Must specify a serializer!");
            } else {
                _ser = options.serializer;
            }
        }

        @Override
        public IMapState makeState(IRunContext context) {
        	Map conf = context.getStormConf();
        	
            RedisState s = new RedisState(makeRedisClient(_servers), _opts, _ser);
            return new MetricsAwareCacheMap(s, _opts.localCacheSize, conf);
        }
        
        public String toString() {
        	return "RedisState.Factory@" + this.hashCode() + " servers: " + _servers;
        }
    }
    
    public static ShardedJedis makeRedisClient(String servers) {
    	List<JedisShardInfo> shards = new ArrayList<JedisShardInfo>();

    	for (String s : servers.split(",")) {
    		String[] shard_data = s.split(":");
    		int port = 6379;
    		if (shard_data.length > 1) {
    			port = Integer.parseInt(shard_data[1]);
    		}
    		shards.add(new JedisShardInfo(shard_data[0], port, s));
    	}

    	return new ShardedJedis(shards);
    }
    
    private final ShardedJedis _client;
    private RedisOptions _opts;
    private ISerializer _ser;
    
    public RedisState(ShardedJedis client, RedisOptions opts, ISerializer<T> ser) {
        _client = client;
        _opts = opts;
        _ser = ser;
    }
    
    String flattenKey(List<Object> keys) {
		if (_opts.key_serializer != null) {
			return new String(_opts.key_serializer.serialize(keys));
		}
    	
    	if (keys.size() == 1) {
    		return keys.get(0).toString();
    	}
    	
    	StringBuilder sb = new StringBuilder();
    	for (int i = 0; i < keys.size(); i++) {
    		sb.append((String)keys.get(i));
    		if (i + 1 < keys.size()) {
    			sb.append(_opts.sep);
    		}
    	}
    	
    	return sb.toString();
    }

    Pipeline getPipeline(Map<Jedis, Pipeline> state, String flat) {
    	Jedis shard = _client.getShard(flat);
//    	System.out.println("getPipeLine: " + flat + " " + shard.getClient().getHost());
    	Pipeline p = state.get(shard);
    	if (p == null) {
    		if (_opts.dbNum != 0) {
    			shard.select(_opts.dbNum);
    		}
    		p = shard.pipelined();
    		state.put(shard, p);
    	}
    	return p;
    }

    @Override
    public List<T> multiGet(List<List<Object>> keys) {
    	Map<Jedis, Pipeline> state = new HashMap<Jedis, Pipeline>();
    	List<Response<byte[]>> responses = new ArrayList<Response<byte[]>>(keys.size());
    	
    	// Map each key to a shard and queue up a pipeline
    	for (List<Object> k : keys) {
    		// Flatten the key
    		String flat = flattenKey(k);
//    		System.out.println("RedisState.multiGet: " + flat);
    		
    		// Get the Pipeline
    		Pipeline p = getPipeline(state, flat);
    		// Add a get.
    		responses.add(p.get(flat.getBytes()));
    	}
    	
    	// Sync all the pipelines.
    	for (Pipeline p : state.values()) {
    		p.sync();
    	}
    	
    	// Pull the results.
    	List<T> ret = new ArrayList(keys.size());
    	for (Response<byte[]> r : responses) {
    		
    		byte[] res = r.get();
//    		System.out.println("RedisState.multiGet: res.length " + (res == null ? null : res.length));
    		if (res != null) {
                T val = (T)_ser.deserialize(res);
//                if (keys.get(ret.size()).toString().equals("[Mitt_Romney]")) {
//                	System.err.println("get '" + keys.get(ret.size()) + "' = " + res + " : " + val);
//                }
                ret.add(val);
              } else {
                ret.add(null);
              }
    	}
    	
        return ret;
    }

    @Override
    public void multiPut(List<List<Object>> keys, List<T> vals) {
    	Map<Jedis, Pipeline> state = new HashMap<Jedis, Pipeline>();
    	
    	// Map each key to a shard and queue up a pipeline
    	for (int i = 0; i < keys.size(); i++) {
    		// Flatten the key
    		String flat = flattenKey(keys.get(i));
    		
    		// Get the Pipeline
    		Pipeline p = getPipeline(state, flat);
    		
    		// Put the value.
    		T val = vals.get(i);
    		byte[] serialized = _ser.serialize(val);
//            if (keys.get(i).toString().equals("[Mitt_Romney]")) {
//            	System.err.println("set '" + keys.get(i) + "' " + flat + " = " + new String(serialized) + " : " + val);
//            }
    		p.set(flat.getBytes(), serialized);
    		if (_opts.expiration != 0) {
    			p.expire(flat, _opts.expiration);
    		}
    	}
    	
    	// Sync all the pipelines.
    	for (Pipeline p : state.values()) {
    		p.sync();
    	}
    }

	@Override
	public void commit(long txid) {
		
	}
}
