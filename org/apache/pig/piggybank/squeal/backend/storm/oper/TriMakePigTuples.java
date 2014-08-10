package org.apache.pig.backend.storm.oper;

import java.util.ArrayList;
import java.util.List;

import org.apache.pig.backend.storm.io.SpoutWrapper;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.io.NullableTuple;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

public class TriMakePigTuples extends BaseFunction {
	
	Integer POS = new Integer(1);
	Integer NEG = new Integer(-1);
	private TupleFactory tf;
	
	@Override
	public void prepare(java.util.Map conf, TridentOperationContext context) {
		 tf = TupleFactory.getInstance();
	}
		
	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		List<Object> ret_arr = new ArrayList<Object>(tuple.size());
		
		for (Object o : tuple.getValues()) {
			if (o instanceof byte[]) {
				ret_arr.add(new DataByteArray((byte[]) o));
			} else {
				ret_arr.add(o);
			}
		}
		
		collector.emit(new Values(null, new NullableTuple(tf.newTupleNoCopy(ret_arr)), POS));
	}

}
