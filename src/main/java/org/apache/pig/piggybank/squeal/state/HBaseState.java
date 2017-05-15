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

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.pig.piggybank.squeal.backend.storm.state.IUDFExposer;
import org.apache.pig.piggybank.squeal.backend.storm.state.MetricsAwareCacheMap;
import org.apache.pig.piggybank.squeal.flexy.components.IMapState;
import org.apache.pig.piggybank.squeal.flexy.components.IRunContext;
import org.apache.pig.piggybank.squeal.flexy.components.ISerializer;
import org.apache.pig.piggybank.squeal.flexy.components.IStateFactory;

public class HBaseState<T> implements IMapState<T> {

	/*
	 * Options for the HBase state.
	 *  - localCacheSize - Number of elements to hold in memory.
	 *  - serialize - Used for object marshaling.
	 *  - columnQualifier - optional qualfier.
	 *  - sep - Compound key separator.
	 */
	public static class HBaseOptions<T> implements Serializable {
        public int localCacheSize = 1000;
        public ISerializer<T> serializer = null;
        public ISerializer key_serializer = null;
        public String sep = "|";        
        public String columnQualifier = "cq";
        public boolean autoFlush = false;
        public boolean skipWAL = false;
    }
	
	public static IStateFactory fromJSONArgs(HashMap args) {
		// Create a default options:
		HBaseOptions opts = new HBaseOptions();
		
		// Pull the table name.
		String tableName = (String) args.get("tableName");
		// Pull the column family.
		String columnFamily = (String) args.get("columnFamily");	
		
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

		if (args.get("sep") != null) {
			opts.sep = (String) args.get("sep");
		}
		if (args.get("columnQualifier") != null) {
			opts.columnQualifier = (String) args.get("columnQualifier");
		}
		if (args.get("autoFlush") != null) {
			opts.autoFlush = ((String) args.get("autoFlush")).equalsIgnoreCase("true");
		}
		if (args.get("skipWAL") != null) {
			opts.skipWAL = ((String) args.get("skipWAL")).equalsIgnoreCase("true");
		}
		
		return new Factory(tableName, columnFamily, opts);
	}

    /*
     * Factory for creating RedisStates.
     */
    protected static class Factory implements IStateFactory, IUDFExposer {
    	
        ISerializer _ser;
        HBaseOptions _opts;

		private String _tableName;
		private String _columnFamily;

        public Factory(String tableName, String columnFamily, HBaseOptions options) {
            _tableName = tableName;
            _columnFamily = columnFamily;
            _opts = options;
            if(options.serializer==null) {
            	throw new RuntimeException("Must specify a serializer");
            } else {
                _ser = options.serializer;
            }
        }

        @Override
        public IMapState makeState(IRunContext context) {
        	Map conf = context.getStormConf();

            HBaseState s = new HBaseState(_tableName, _columnFamily, _opts, _ser);
            return new MetricsAwareCacheMap(s, _opts.localCacheSize, conf);
        }
        
        public String toString() {
        	return "HBaseState.Factory@" + this.hashCode() + " tableName: " + _tableName + " columnFamily: " + _columnFamily;
        }

		@Override
		public Collection<? extends String> getUDFs() {
			List<String> udfs = new ArrayList<String>();
			
			udfs.add(Bytes.class.getName());
			udfs.add(HTable.class.getName());
            udfs.add("org.apache.hadoop.hbase.protobuf.generated.MasterProtos");
            udfs.add("org.cloudera.htrace.Trace");
			
			return udfs;
		}
    }
    
    private HBaseOptions _opts;
    private ISerializer _ser;
	private byte[] _tableName;
	private byte[] _columnFamily;
	private byte[] _columnQualifier;
	private HTable table;
	private String tableName;
	private String columnFamily;
    
    public HBaseState(String tableName, String columnFamily, HBaseOptions opts, ISerializer<T> ser) {
        _tableName = Bytes.toBytes(tableName);
        this.tableName = tableName;
        _columnFamily = Bytes.toBytes(columnFamily);
        this.columnFamily = columnFamily;
        _opts = opts;
        _columnQualifier = Bytes.toBytes(opts.columnQualifier);
        _ser = ser;

        // Make the table object.
        Configuration config = HBaseConfiguration.create();
        try {
			table = new HTable(config, _tableName);
			if (!opts.autoFlush) {
				table.setAutoFlush(false);
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
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
	
	@Override
	public List<T> multiGet(List<List<Object>> keys) {
		
		List<Get> gets = new ArrayList<Get>(keys.size());
		// Create a Get for each.
    	for (int i = 0; i < keys.size(); i++) {
    		// Flatten the key
    		String flat = flattenKey(keys.get(i));
    		Get g = new Get(Bytes.toBytes(flat));
    		
    		// Filter as needed.
    		g.addColumn(_columnFamily, _columnQualifier);
    		
    		gets.add(g);
    	}
    	
    	Result[] results;
    	try {
			results = table.get(gets);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		
		// Pull the results.
    	List<T> ret = new ArrayList(keys.size());
    	for (Result r : results) {
    		
    		byte[] res = r.getValue(_columnFamily, _columnQualifier);
//    		System.out.println("HBaseState.multiGet: res.length " + (res == null ? null : res.length));
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
		List<Put> puts = new ArrayList<Put>(keys.size());
		
		// Map each key to a shard and queue up a pipeline
    	for (int i = 0; i < keys.size(); i++) {
    		// Flatten the key
    		String flat = flattenKey(keys.get(i));
    		Put p = new Put(Bytes.toBytes(flat));
    		if (_opts.skipWAL) {
    			p.setDurability(Durability.SKIP_WAL);
    		}
    		
    		
    		// Put the value.
    		T val = vals.get(i);
    		byte[] serialized = _ser.serialize(val);
//            if (keys.get(i).toString().equals("[Mitt_Romney]")) {
//            	System.err.println("set '" + keys.get(i) + "' " + flat + " = " + new String(serialized) + " : " + val);
//            }
    		
    		p.add(_columnFamily, _columnQualifier, serialized);

            puts.add(p);
    	}
    	
    	try {
			table.put(puts);
			if (!_opts.autoFlush) {
				table.flushCommits();
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	public String toString() {
    	return "HBaseState@" + this.hashCode() + " tableName: " + tableName + " columnFamily: " + columnFamily;
    }

	@Override
	public void commit(long txid) {
		
	}

}
