package org.apache.pig.backend.storm.io;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.pig.backend.hadoop.HDataType;
import org.apache.pig.backend.storm.state.StateWrapper;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.io.PigNullableWritable;

import backtype.storm.tuple.Values;
import storm.trident.state.StateFactory;
import storm.trident.state.map.MapState;

public class StateStore extends PigStorage implements ISignStore {

	private String jsonOpts;
	private AtomicInteger sign;
	private MapState s;

	public StateStore(String jsonOpts) {
		this.jsonOpts = jsonOpts;
	}

	@Override
	public List<String> getUDFs() {
		ArrayList<String> ret = new ArrayList<String>();
		ret.add(new StateWrapper(jsonOpts).getStateFactory().getClass().getName());
		return ret;
	}

	@Override
	public void setSign(AtomicInteger sign) {
		this.sign = sign;
	}

	@Override
	public void putNext(Tuple t) throws IOException {
		if (s == null) {
			StateFactory stateFactory = new StateWrapper(jsonOpts).getStateFactory();
			s = (MapState) stateFactory.makeState(new HashMap(), null, 0, 1);
		}
		
		if (sign.get() < 0) {
			return;
		}
		
		// Pull key from tuple(0) and value from tuple(1).
		PigNullableWritable k = HDataType.getWritableComparableTypes(t.get(0), DataType.findType(t.get(0)));
		
		PigNullableWritable v = HDataType.getWritableComparableTypes(t.get(1), DataType.findType(t.get(1)));
		
		s.multiPut(new Values(new Values(k)), new Values(new Values(v)));
	}
}
