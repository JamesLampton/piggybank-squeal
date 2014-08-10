package org.apache.pig.backend.storm.state;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.pig.impl.io.NullableTuple;
import org.apache.pig.impl.util.Pair;

public interface IPigIdxState<T> extends Writable {

	public abstract List<NullableTuple> getTuples(Text which);
	public abstract List<Pair<List<NullableTuple>, List<NullableTuple>>> getTupleBatches(T lastState);
	public Pair<Writable, List<Writable>> separate(List<Integer[]> bins);
	public void merge(IPigIdxState other);
	
}
