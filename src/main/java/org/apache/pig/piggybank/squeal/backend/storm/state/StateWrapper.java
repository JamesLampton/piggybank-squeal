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
import org.apache.pig.piggybank.squeal.flexy.components.IMapState;
import org.apache.pig.piggybank.squeal.flexy.components.IRunContext;
import org.apache.pig.piggybank.squeal.flexy.components.IStateFactory;
import org.apache.pig.piggybank.squeal.flexy.components.impl.FakeRunContext;
import org.apache.pig.piggybank.squeal.flexy.model.FFields;
import org.apache.pig.piggybank.squeal.flexy.model.FStream;
import org.apache.pig.piggybank.squeal.state.LRUMapState;
import org.mortbay.util.ajax.JSON;

import backtype.storm.task.IMetricsContext;
import backtype.storm.task.TopologyContext;

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
	
	public IStateFactory getStateFactory() {
		return getStateFactoryFromArgs(stateFactoryCN, staticMethod, args);
	}
	
	public static IStateFactory getStateFactoryFromArgs(String stateFactoryCN, String staticMethod, Object[] args) {
		if (stateFactoryCN == null) {
			return new LRUMapState.Factory(100);
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
				return (IStateFactory) m.invoke(cls, args);
			} else {			
				if (args != null) {
					Constructor<?> constr = cls.getConstructor(cls_arr);
					return (IStateFactory) constr.newInstance(args);
				} else {
					return (IStateFactory) cls.newInstance();
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
		IStateFactory sf = sw.getStateFactory();
		IMapState s = (IMapState) sf.makeState(new FakeRunContext());
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
