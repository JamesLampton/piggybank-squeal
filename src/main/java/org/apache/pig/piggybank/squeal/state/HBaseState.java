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
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.pig.piggybank.squeal.backend.storm.state.IUDFExposer;

import backtype.storm.task.IMetricsContext;
import backtype.storm.tuple.Values;
import storm.trident.state.JSONNonTransactionalSerializer;
import storm.trident.state.JSONOpaqueSerializer;
import storm.trident.state.JSONTransactionalSerializer;
import storm.trident.state.OpaqueValue;
import storm.trident.state.Serializer;
import storm.trident.state.State;
import storm.trident.state.StateFactory;
import storm.trident.state.StateType;
import storm.trident.state.TransactionalValue;
import storm.trident.state.map.CachedMap;
import storm.trident.state.map.IBackingMap;
import storm.trident.state.map.MapState;
import storm.trident.state.map.NonTransactionalMap;
import storm.trident.state.map.OpaqueMap;
import storm.trident.state.map.SnapshottableMap;
import storm.trident.state.map.TransactionalMap;

public class HBaseState<T> implements IBackingMap<T> {

	/*
	 * Options for the HBase state.
	 *  - localCacheSize - Number of elements to hold in memory.
	 *  - globlKey - Used for the SnapshottableMap
	 *  - serialize - Used for object marshaling.
	 *  - columnQualifier - optional qualfier.
	 *  - sep - Compound key separator.
	 */
	public static class HBaseOptions<T> implements Serializable {
        public int localCacheSize = 1000;
        public String globalKey = "$GLOBAL$";
        public Serializer<T> serializer = null;
        public Serializer key_serializer = null;
        public String sep = "|";        
        public String columnQualifier = "cq";
    }
	
	public static StateFactory fromJSONArgs(HashMap args) {
		// Create a default options:
		HBaseOptions opts = new HBaseOptions();
		// Specify a default storage type:
		String storage_type = "NON_TRANSACTIONAL";
		
		// Pull the table name.
		String tableName = (String) args.get("tableName");
		// Pull the column family.
		String columnFamily = (String) args.get("columnFamily");	
		
		// Pull out non-default stuff.
		if (args.get("localCacheSize") != null) {
			opts.localCacheSize = Integer.parseInt(args.get("localCacheSize").toString());
		}
		if (args.get("globalKey") != null) {
			opts.globalKey = (String) args.get("globalKey");
		}
		if (args.get("serializer") != null) {
			String cn = (String) args.get("serializer");
			// Special case here -- pull the class name and set it up.
			try {
				Class<?> cls = Class.forName(cn);
				opts.serializer = (Serializer) cls.newInstance();	
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
		if (args.get("key_serializer") != null) {
			String cn = (String) args.get("key_serializer");
			// Special case here -- pull the class name and set it up.
			try {
				Class<?> cls = Class.forName(cn);
				opts.key_serializer = (Serializer) cls.newInstance();	
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
				
		
		if (storage_type.equalsIgnoreCase("NON_TRANSACTIONAL")) {
			return nonTransactional(tableName, columnFamily, opts);
		} else if (storage_type.equalsIgnoreCase("OPAQUE")) {
			return opaque(tableName, columnFamily, opts);
		} else if (storage_type.equalsIgnoreCase("TRANSACTIONAL")) {
			return transactional(tableName, columnFamily, opts);
		} else {
			throw new RuntimeException("Unknown storage type: " + storage_type);
		}
	}

	/*
	 * Helper routines for creating factories.
	 */
	// opaque
    public static StateFactory opaque(String tableName, String columnFamily) { return opaque(tableName, columnFamily, new HBaseOptions()); }
    public static StateFactory opaque(String tableName, String columnFamily, HBaseOptions<OpaqueValue> opts) {
        return new Factory(tableName, columnFamily, StateType.OPAQUE, opts);
    }
    // Transactional
    public static StateFactory transactional(String tableName, String columnFamily) { return transactional(tableName, columnFamily, new HBaseOptions()); }
    public static StateFactory transactional(String tableName, String columnFamily, HBaseOptions<TransactionalValue> opts) {
        return new Factory(tableName, columnFamily, StateType.TRANSACTIONAL, opts);
    }
    // nonTransactional
    public static StateFactory nonTransactional(String tableName, String columnFamily) { return nonTransactional(tableName, columnFamily, new HBaseOptions()); }
    public static StateFactory nonTransactional(String tableName, String columnFamily, HBaseOptions<Object> opts) {
        return new Factory(tableName, columnFamily, StateType.NON_TRANSACTIONAL, opts);
    }

    /*
     * Factory for creating RedisStates.
     */
    protected static class Factory implements StateFactory, IUDFExposer {
    	// Helper structure for easy lookup of serializers.
        private static final Map<StateType, Serializer> DEFAULT_SERIALZERS = new HashMap<StateType, Serializer>() {{
            put(StateType.NON_TRANSACTIONAL, new JSONNonTransactionalSerializer());
            put(StateType.TRANSACTIONAL, new JSONTransactionalSerializer());
            put(StateType.OPAQUE, new JSONOpaqueSerializer());
        }};
    	
        StateType _type;
        Serializer _ser;
        HBaseOptions _opts;

		private String _tableName;
		private String _columnFamily;

        public Factory(String tableName, String columnFamily, StateType type, HBaseOptions options) {
            _type = type;
            _tableName = tableName;
            _columnFamily = columnFamily;
            _opts = options;
            if(options.serializer==null) {
                _ser = DEFAULT_SERIALZERS.get(type);
                if(_ser==null) {
                    throw new RuntimeException("Couldn't find serializer for state type: " + type);
                }
            } else {
                _ser = options.serializer;
            }
        }

        @Override
        public State makeState(Map conf, IMetricsContext m, int partitionIndex, int numPartitions) {
            HBaseState s = new HBaseState(_tableName, _columnFamily, _opts, _ser);
            CachedMap c = new CachedMap(s, _opts.localCacheSize);
            MapState ms;
            if(_type == StateType.NON_TRANSACTIONAL) {
                ms = NonTransactionalMap.build(c);
            } else if(_type==StateType.OPAQUE) {
                ms = OpaqueMap.build(c);
            } else if(_type==StateType.TRANSACTIONAL){
                ms = TransactionalMap.build(c);
            } else {
                throw new RuntimeException("Unknown state type: " + _type);
            }
            return new SnapshottableMap(ms, new Values(_opts.globalKey));
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
    private Serializer _ser;
	private byte[] _tableName;
	private byte[] _columnFamily;
	private byte[] _columnQualifier;
	private HTable table;
    
    public HBaseState(String tableName, String columnFamily, HBaseOptions opts, Serializer<T> ser) {
        _tableName = Bytes.toBytes(tableName);
        _columnFamily = Bytes.toBytes(columnFamily);
        _opts = opts;
        _columnQualifier = Bytes.toBytes(opts.columnQualifier);
        _ser = ser;

        // Make the table object.
        Configuration config = HBaseConfiguration.create();
        try {
			table = new HTable(config, _tableName);
			table.setAutoFlush(false);
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
			table.flushCommits();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

}
