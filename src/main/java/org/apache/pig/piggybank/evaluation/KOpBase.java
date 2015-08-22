/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.pig.piggybank.evaluation;

import java.io.IOException;
import java.lang.reflect.ParameterizedType;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.pig.Accumulator;
import org.apache.pig.Algebraic;
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.DefaultBagFactory;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.io.NullableUnknownWritable;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.piggybank.squeal.AlgebraicInverse;

/**
 * Base functionality to implement things like MIN and MAX with support for inverse values.
 * 
 * @author JamesLampton
 */

public abstract class KOpBase<T> extends EvalFunc<T> implements Algebraic, Accumulator<T> {

	static BagFactory bf = DefaultBagFactory.getInstance();
	static TupleFactory tf = TupleFactory.getInstance();
	
	static final int DEFAULT_INTERMEDIATE = 10;
	
	private int maxIntermediate;
	private boolean runMin;
	private EvalFunc<Tuple> initWrapped;
	private KOpBaseIntermed intermedWrapped;
	private KOpBaseFinal<T> finalWrapped;

	public KOpBase(boolean runMin) {
		this(DEFAULT_INTERMEDIATE, runMin);
	}
	
	public KOpBase(int maxIntermediate, boolean runMin) {
		this.maxIntermediate = maxIntermediate;
		this.runMin = runMin;
		
		// Instantiate our stage variables to be able to execute or accumulate.
		try {
			Class<?> klazz = ClassLoader.getSystemClassLoader().loadClass(getInitial());
			initWrapped = (EvalFunc<Tuple>) klazz.newInstance();
			
			klazz = ClassLoader.getSystemClassLoader().loadClass(getIntermed());
			intermedWrapped = (KOpBaseIntermed) klazz.newInstance();
			intermedWrapped.k = maxIntermediate;
			intermedWrapped.runMin = runMin;
			
			klazz = ClassLoader.getSystemClassLoader().loadClass(getFinal());
			finalWrapped = (KOpBaseFinal<T>) klazz.newInstance();
			finalWrapped.runMin = runMin;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	static Tuple wrap(Tuple init, long sign) throws ExecException {
		// Wrap the value in a bag.
		Tuple tup = tf.newTuple(2);
		tup.set(0, init.get(0));
		tup.set(1, sign);
		
		DataBag bag = bf.newDefaultBag();
		bag.add(tup);
		
		return tf.newTuple(bag);
	}
	
	static Tuple merge(Tuple input, boolean runMin, int k) throws ExecException {
		Map<Object, Long> vals = new HashMap<Object, Long>();
		
		// Merge the bags into one.
		for (Tuple outerT : (DataBag) input.get(0)) {
			for (Tuple t : (DataBag) outerT.get(0)) {
				Long cur = vals.remove(t.get(0));
				cur = (cur == null ? 0 : cur) + (Long) t.get(1);
				if (cur != 0) {
					vals.put(t.get(0), cur);
				}
			}
		}
		
		// Pull the keys.
		ArrayList keyList = new ArrayList(vals.keySet());
		Collections.sort(keyList);
		if (runMin) {
			Collections.reverse(keyList);
		}
		
		// now keep the top/bottom k.
		DataBag ret = bf.newDefaultBag();
		for (Object elem : keyList) {
			Tuple tup = tf.newTuple(2);
		
			tup.set(0, elem);
			tup.set(1, vals.get(elem));
			
			ret.add(tup);
			if (ret.size() >= k) {
				break;
			}
		}
		
		return tf.newTuple(ret);
	}
	
	static public class KOpBaseInitial extends EvalFunc<Tuple> implements AlgebraicInverse {
		@Override
		public String getInitialInverse() { return KOpBaseInitialInverse.class.getName(); }

		public int getSign() {
    		return 1;
    	}
		
		@Override
		public Tuple exec(Tuple input) throws IOException {
//			System.err.println("INIT: " + input);
			for (Tuple t : (DataBag) input.get(0)) {
				return wrap(t, getSign());	
			}
			// Odd.
			return tf.newTuple(bf.newDefaultBag());
		}
	}
	
	static public class KOpBaseInitialInverse extends KOpBaseInitial {
    	public int getSign() {
    		return -1;
    	}

		@Override
		public String getInitialInverse() {	throw new RuntimeException("This shouldn't happen."); }
    }
	
	static public class KOpBaseIntermed extends EvalFunc<Tuple> {
		private boolean runMin;
		private int k;

		public KOpBaseIntermed() {
			
		}
		
		public KOpBaseIntermed(int k, boolean runMin) {
			this.runMin = runMin;
			this.k = k;
		}
		
		@Override
		public Tuple exec(Tuple input) throws IOException {
//			System.err.println("INTERMED: " + input);
			return merge(input, runMin, k);
		}
	}
	
	static public class KOpBaseFinal<T> extends EvalFunc<T> {
		
		private boolean runMin;

		public KOpBaseFinal() {
			
		}
		
		public KOpBaseFinal(boolean runMin) {
			this.runMin = runMin;
		}
		
		@SuppressWarnings("unchecked")
		@Override
		public T exec(Tuple input) throws IOException {
//			System.err.println("FINAL: " + input);
			Object ret = null;
			
			// Cycle through.
			for (Tuple outerTuple : (DataBag) input.get(0)) {
				for (Tuple tup : (DataBag) outerTuple.get(0)) {
//					System.err.println("OP cur: " + ret + " check: " + tup);
					if (0 <= (Long) tup.get(1)) {
						if (ret == null) {
							ret = tup.get(0);
						} else {
							int cmp = DataType.compare(ret, tup.get(0));
							if ((runMin && cmp > 0) || (!runMin && cmp < 0)) {
								ret = tup.get(0);
							}
						}
					}
				}
			}
			
			return (T) ret;
		}
		
	}
	
	@Override
	public String getInitial() { return KOpBaseInitial.class.getName(); }
	@Override
	abstract public String getIntermed();
	@Override
	abstract public String getFinal();

	@Override
	public T exec(Tuple input) throws IOException {
		DataBag data = (DataBag) input.get(0);
		for (Tuple t : data) {
			accumulate(t);
		}
		T ret = getValue();
		cleanup();
		
		return ret;
	}

	Tuple value = null;
	
	@Override
	public void accumulate(Tuple b) throws IOException {
		Tuple inited = initWrapped.exec(b);
		if (value == null) {
			value = inited;
		} else {
			DataBag bag = bf.newDefaultBag();
			bag.add(value);
			bag.add(inited);
			value = intermedWrapped.exec(tf.newTuple(bag));
		}
	}

	@Override
	public T getValue() {
		try {
			return finalWrapped.exec(value);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void cleanup() {
		value = null;
	}

	@Override
    public Schema outputSchema(Schema input) {
		// Pull the generic parameter to determine the return type.
		Class<T> typeOfT = (Class<T>)
                ((ParameterizedType)getClass()
                .getGenericSuperclass())
                .getActualTypeArguments()[0];

      return new Schema(new Schema.FieldSchema(null, DataType.findType(typeOfT)));				
	}
	
	// *********************************************************************************
	// Base code for MIN.
	// *********************************************************************************
	
	static public class KOpMinIntermed extends KOpBaseIntermed {
		public KOpMinIntermed() {
			super(DEFAULT_INTERMEDIATE, true);
		}
	}
	
	static public class KMINFinal<T2> extends KOpBaseFinal<T2> {
		public KMINFinal() {
			super(true);
		}
	}
	
	abstract static public class KMINBase<T2> extends KOpBase<T2> {
		public KMINBase() {
			this(Integer.toString(DEFAULT_INTERMEDIATE));
		}
		
		public KMINBase(String maxIntermed) {
			super(Integer.parseInt(maxIntermed), true);
		}

		@Override
		public String getIntermed() {
			return KOpMinIntermed.class.getName();
		}
	}
	
	// *********************************************************************************
	// Base code for MAX.
	// *********************************************************************************

	static public class KOpMaxIntermed extends KOpBaseIntermed {
		public KOpMaxIntermed() {
			super(DEFAULT_INTERMEDIATE, false);
		}
	}
	
	static public class KMAXFinal<T2> extends KOpBaseFinal<T2> {
		public KMAXFinal() {
			super(false);
		}
	}

	abstract static public class KMAXBase<T2> extends KOpBase<T2> {
		public KMAXBase() {
			this(Integer.toString(DEFAULT_INTERMEDIATE));
		}

		public KMAXBase(String maxIntermed) {
			super(Integer.parseInt(maxIntermed), false);
		}

		@Override
		public String getIntermed() {
			return KOpMaxIntermed.class.getName();
		}
	}
}
