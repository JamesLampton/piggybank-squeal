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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.pig.piggybank.squeal.flexy.oper.CombinePersist;
import org.apache.pig.impl.io.NullableTuple;
import org.apache.pig.impl.util.Pair;

public class CombineTupleWritable implements Writable, IPigIdxState<CombineTupleWritable> {
	private List<Writable> values;
	
	public CombineTupleWritable() {
		
	}

	public CombineTupleWritable(Writable[] vals) {
		values = Arrays.asList(vals);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(values.size());
		for (int i = 0; i < values.size(); ++i) {
			Text.writeString(out, values.get(i).getClass().getName());
		}
		for (int i = 0; i < values.size(); ++i) {
			values.get(i).write(out);
		}
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		int card = in.readInt();
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
		return CombinePersist.getTuples(this);
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
