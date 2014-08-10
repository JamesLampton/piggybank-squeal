package org.apache.pig.backend.storm.state;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.HDataType;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.PigNullableWritable;
import org.mortbay.util.ajax.JSON;

import storm.trident.state.StateFactory;
import storm.trident.state.map.MapState;
import storm.trident.testing.LRUMemoryMapState;

public class StateWrapper {
	private String stateFactoryCN;
	private Object[] args;
	private String staticMethod;
	private String jsonOpts;
	
	public StateWrapper(String jsonOpts) {
		this.jsonOpts = jsonOpts;
		
		// Decode jsonOpts
		if (jsonOpts != null) {
			Map<String, Object> m = (Map<String, Object>) JSON.parse(jsonOpts);

			stateFactoryCN = (String) m.get("StateFactory");
			args = (Object[]) m.get("args");
			staticMethod = (String) m.get("StaticMethod");
		}
//		this.stateFactoryCN = stateFactoryCN;
//		this.staticMethod = staticMethod;
//		this.jsonArgs = jsonArgs;
	}
	
	public StateFactory getStateFactory() {
		return getStateFactoryFromArgs(stateFactoryCN, staticMethod, args);
	}
	
	public static StateFactory getStateFactoryFromArgs(String stateFactoryCN, String staticMethod, Object[] args) {
		if (stateFactoryCN == null) {
			return new LRUMemoryMapState.Factory(2000);
		}
		
		try {
			Class<?> cls = PigContext.getClassLoader().loadClass(stateFactoryCN);
			
			Class<?> cls_arr[] = null;
			if (args != null) {
				cls_arr = new Class<?>[args.length];
				for (int i = 0; i < args.length; i++) {
					cls_arr[i] = args[i].getClass();
				}
			}
			
			if (staticMethod != null) {
				Method m = cls.getMethod(staticMethod, cls_arr);
				return (StateFactory) m.invoke(cls, args);
			} else {			
				if (args != null) {
					Constructor<?> constr = cls.getConstructor(cls_arr);
					return (StateFactory) constr.newInstance(args);
				} else {
					return (StateFactory) cls.newInstance();
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}
	
	public static void main(String args[]) {
		if (args.length == 0) {
			System.err.println("Usage: " + StateWrapper.class.getName() + " <jsonArgs> <key>");
			return;
		}
		
		StateWrapper sw = new StateWrapper(args[0]);
		StateFactory sf = sw.getStateFactory();
		MapState s = (MapState) sf.makeState(new HashMap(), null, 0, 1);
		List<List<Object>> keys = new ArrayList<List<Object>>();
		
		ArrayList<Object> key = new ArrayList<Object>();
		key.add(args[1]);
		
		Tuple t = TupleFactory.getInstance().newTupleNoCopy(key);
		PigNullableWritable tw;
		try {
//			tw = HDataType.getWritableComparableTypes(t, DataType.findType(t));
			tw = HDataType.getWritableComparableTypes(args[1], DataType.findType(args[1]));
		} catch (ExecException e) {
			throw new RuntimeException(e);
		}
		List<Object> l = new ArrayList<Object>();
		l.add(tw);
		keys.add(l);
		
		System.out.println("Key: " + tw);
		
		System.out.println(s.multiGet(keys));
	}
}
