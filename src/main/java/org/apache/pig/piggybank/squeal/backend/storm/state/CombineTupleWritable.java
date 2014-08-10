package org.apache.pig.backend.storm.state;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.pig.backend.storm.oper.TriCombinePersist;
import org.apache.pig.impl.io.NullableTuple;
import org.apache.pig.impl.util.Pair;

import backtype.storm.utils.WritableUtils;

public class CombineTupleWritable implements Writable, IPigIdxState<CombineTupleWritable> {
	private List<Writable> values;
	
	public CombineTupleWritable() {
		
	}

	public CombineTupleWritable(Writable[] vals) {
		values = Arrays.asList(vals);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		WritableUtils.writeVInt(out, values.size());
		for (int i = 0; i < values.size(); ++i) {
			Text.writeString(out, values.get(i).getClass().getName());
		}
		for (int i = 0; i < values.size(); ++i) {
			values.get(i).write(out);
		}
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		int card = WritableUtils.readVInt(in);
		values = new ArrayList<Writable>(card);
		Class<? extends Writable>[] cls = new Class[card];
		try {
			for (int i = 0; i < card; ++i) {
				cls[i] = Class.forName(Text.readString(in)).asSubclass(Writable.class);
			}
			for (int i = 0; i < card; ++i) {
				values.add(i, cls[i].newInstance());
				values.get(i).readFields(in);
			}
		} catch (ClassNotFoundException e) {
			throw (IOException)new IOException("Failed tuple init").initCause(e);
		} catch (IllegalAccessException e) {
			throw (IOException)new IOException("Failed tuple init").initCause(e);
		} catch (InstantiationException e) {
			throw (IOException)new IOException("Failed tuple init").initCause(e);
		}
	}

	public Object get(int i) {
		return values.get(i);
	}
	
	public String toString() {
		return "CombineTupleWritable(" + values.toString() + ")";
	}

	@Override
	public List<NullableTuple> getTuples(Text which) {
		return TriCombinePersist.getTuples(this);
	}

	@Override
	public Pair<Writable, List<Writable>> separate(List<Integer[]> bins) {
		throw new RuntimeException("Not implemented for combined plans.");
	}

	@Override
	public void merge(IPigIdxState other) {
		throw new RuntimeException("Not implemented for combined plans.");		
	}

	@Override
	public List<Pair<List<NullableTuple>, List<NullableTuple>>> getTupleBatches(
			CombineTupleWritable lastState) {
		List<NullableTuple> first = null;
		if (lastState != null) {
			first = lastState.getTuples(null);
		}
		List<NullableTuple> second = getTuples(null);

		Pair<List<NullableTuple>, List<NullableTuple>> p = 
				new Pair<List<NullableTuple>, List<NullableTuple>>(first, second);
		
		ArrayList<Pair<List<NullableTuple>, List<NullableTuple>>> ret = 
				new ArrayList<Pair<List<NullableTuple>, List<NullableTuple>>>();
		ret.add(p);
		return ret;
	}
}
