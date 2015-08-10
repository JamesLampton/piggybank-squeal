package org.apache.pig.piggybank.squeal.builtin;

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
import org.apache.pig.data.DefaultBagFactory;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.piggybank.squeal.AlgebraicInverse;

public abstract class MinMaxDoppleganger<T,T2> extends EvalFunc<T2> implements Algebraic, Accumulator<T2> {

	public MinMaxDoppleganger() {
		Class<T> typeOfT = (Class<T>)
                ((ParameterizedType)getClass()
                .getGenericSuperclass())
                .getActualTypeArguments()[0];
		
		// Instantiate what we've wrapped.
		try {
			wrapped = typeOfT.newInstance();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	
	static BagFactory bf = DefaultBagFactory.getInstance();
	static TupleFactory tf = TupleFactory.getInstance();
	
	static public Tuple wrap(Tuple init, long sign) throws ExecException {
		// Wrap the value in a bag.
		Tuple tup = tf.newTuple(2);
		tup.set(0, init.get(0));
		tup.set(1, sign);
		
		DataBag bag = bf.newDefaultBag();
		bag.add(tup);
		
		return tf.newTuple(bag);
	}
	
	static public Tuple merge(Tuple input, boolean reversed, int k) throws ExecException {
		Map<Object, Long> vals = new HashMap<Object, Long>();
		
		DataBag bags = (DataBag) input.get(0);
		for (Tuple t : bags) {
			Long cur = vals.remove(t.get(0));
			cur = (cur == null ? 0 : cur) + (Long) t.get(1);
			if (cur != 0) {
				vals.put(t.get(0), cur);
			}
		}
		
		// Pull the keys.
		ArrayList keyList = new ArrayList(vals.keySet());
		Collections.sort(keyList);
		if (reversed) {
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
	
	static Tuple unroll(Tuple input) throws ExecException {
		DataBag ret = bf.newDefaultBag();
		
		for (Tuple tup : (DataBag) input.get(0)) {
			if (0 <= (Long) tup.get(1)) {
				ret.add(tf.newTuple(tup.get(0)));
			}
		}
		
		return tf.newTuple(ret);
	}
	
	
	static public class DopInitialInverse<T> extends DopInitial<T> {
    	public int getSign() {
    		return -1;
    	}
    }
	
	static class DopInitial<T> extends EvalFunc<Tuple> implements AlgebraicInverse {

		private EvalFunc<Tuple> wrapped;

		public DopInitial() {
			Class<T> typeOfT = (Class<T>)
	                ((ParameterizedType)getClass()
	                .getGenericSuperclass())
	                .getActualTypeArguments()[0];
			
			// Instantiate what we've wrapped.
			try {
				Class<?> klazz = ClassLoader.getSystemClassLoader().loadClass(
						((Algebraic)typeOfT.newInstance()).getInitial());
				wrapped = (EvalFunc<Tuple>) klazz.newInstance();
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
		
		public int getSign() {
    		return 1;
    	}
		
		@Override
		public Tuple exec(Tuple input) throws IOException {
			return wrap(wrapped.exec(input), getSign());
		}

		@Override
		public String getInitialInverse() {
			throw new RuntimeException("You must subclass and override this function.");
		}	
	}
	
	static class DopIntermed extends EvalFunc<Tuple> {
		private boolean reversed;
		private int k;

		public DopIntermed(boolean reversed, int k) {
			this.reversed = reversed;
			this.k = k;
		}
		
		@Override
		public Tuple exec(Tuple input) throws IOException {
			return merge(input, reversed, k);
		}
	}
	
	static class DopFinal<T,T2> extends EvalFunc<T2> {
		private EvalFunc<T2> wrapped;
		
		public DopFinal() {
			Class<T> typeOfT = (Class<T>)
	                ((ParameterizedType)getClass()
	                .getGenericSuperclass())
	                .getActualTypeArguments()[0];
			
			// Instantiate what we've wrapped.
			try {
				Class<?> klazz = ClassLoader.getSystemClassLoader().loadClass(
						((Algebraic)typeOfT.newInstance()).getFinal());
				wrapped = (EvalFunc<T2>) klazz.newInstance();
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
		
		@Override
		public T2 exec(Tuple input) throws IOException {
			return (T2) wrapped.exec(unroll(input));
		}
	}
	
	private T wrapped;

	@Override
	public void accumulate(Tuple b) throws IOException {
		((Accumulator)wrapped).accumulate(b);
	}

	@Override
	public T2 getValue() {
		return ((Accumulator<T2>)wrapped).getValue();
	}

	@Override
	public void cleanup() {
		((Accumulator)wrapped).cleanup();
	}

	// We need parameterization for this -- subclass above.
	@Override
	abstract public String getInitial();

	// Subclass and set reversed.
	@Override
	abstract public String getIntermed();

	// We need parameterization for this.
	@Override
	abstract public String getFinal();

	@Override
	public T2 exec(Tuple input) throws IOException {
		return ((EvalFunc<T2>)wrapped).exec(input);
	}
	
	@Override
    public Schema outputSchema(Schema input) {
        return ((EvalFunc) wrapped).outputSchema(input);
    }
}
