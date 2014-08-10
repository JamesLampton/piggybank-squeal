package org.apache.pig.backend.storm.state;

import java.util.List;

import org.apache.hadoop.io.MapWritable;
import org.apache.pig.impl.io.NullableTuple;
import org.apache.pig.impl.util.Pair;

public abstract class MapIdxWritable<T> extends MapWritable implements IPigIdxState<T> {

	public abstract List<Pair<List<NullableTuple>, List<NullableTuple>>> getTupleBatches(T last);
	
	public String toString() {
		return this.getClass().getSimpleName() + "@" + this.hashCode() + ": " + this.entrySet();
	}
}
