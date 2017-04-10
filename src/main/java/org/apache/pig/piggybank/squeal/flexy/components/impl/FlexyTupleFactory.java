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

package org.apache.pig.piggybank.squeal.flexy.components.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.Writable;
import org.apache.pig.piggybank.squeal.flexy.components.IFlexyTuple;
import org.apache.pig.piggybank.squeal.flexy.model.FFields;

public class FlexyTupleFactory {

	class FlexyTupleImpl implements IFlexyTuple {

		private ArrayList<Object> values;
		private IFlexyTuple parent;

		public FlexyTupleImpl(IFlexyTuple parent, List<Object> values) {
			this.parent = parent;
			this.values = new ArrayList<Object>(values);
		}

		@Override
		public boolean isEmpty() {
			return values.isEmpty() || (parent == null ? false : parent.isEmpty());
		}

		@Override
		public boolean contains(Object o) {
			return values.contains(o) || (parent == null ? false : parent.contains(o));
		}

		@Override
		public Iterator iterator() {
			// TODO Auto-generated method stub with exception
			throw new RuntimeException("Not implemented");
		}

		@Override
		public Object[] toArray() {
			return getValues().toArray();
		}

		@Override
		public Object[] toArray(Object[] a) {
			return toArray();
		}

		@Override
		public boolean add(Object e) {
			throw new RuntimeException("FlexyTupleImpl are immutable.");
		}

		@Override
		public boolean remove(Object o) {
			throw new RuntimeException("FlexyTupleImpl are immutable.");
		}

		@Override
		public boolean containsAll(Collection c) {
			for (Object o : c) {
				if (!contains(o)) {
					return false;
				}
			}
			return true;
		}

		@Override
		public boolean addAll(Collection c) {
			throw new RuntimeException("FlexyTupleImpl are immutable.");
		}

		@Override
		public boolean removeAll(Collection c) {
			throw new RuntimeException("FlexyTupleImpl are immutable.");
		}

		@Override
		public boolean retainAll(Collection c) {
			throw new RuntimeException("FlexyTupleImpl are immutable.");
		}

		@Override
		public void clear() {
			throw new RuntimeException("FlexyTupleImpl are immutable.");
		}

		@Override
		public Object get(int i) {
			// FIXME: Do a binary sort through the parents.
			if (i >= idx_start) {
				return values.get(i - idx_start);
			}
			if (parent != null) {
				return parent.get(i);
			}
			// Exception will follow.
			return values.get(i);
		}

		@Override
		public int size() {
			return idx_end;
		}

		@Override
		public List<Object> getValues() {
			ArrayList<Object> ret = new ArrayList<Object>(size());
			for (int i = 0; i < size(); i++) {
				ret.add(get(i));
			}
			return ret;
		}

		@Override
		public Object getValueByField(String fName) {
			Integer pos = sch_map.get(fName);
			if (pos != null) {
				return values.get(pos);
			}
			
			if (parent == null) {
				throw new RuntimeException("Requesting field: " + fName + " not found");
			}
			
			return parent.getValueByField(fName);
		}

		@Override
		public int getInteger(int i) {
			return (int) get(i);
		}
		
		@Override
		public byte[] getBinary(int i) {
			return (byte[]) get(i);
		}

		@Override
		public long getLong(int i) {
			return (long) get(i);
		}

		@Override
		public boolean getBoolean(int i) {
			return (boolean) get(i);
		}

		@Override
		public List<Object> select(FFields gr_fields) {
			List<String> gr_list = gr_fields.toList();
			List<Object> ret = new ArrayList<Object>(gr_list.size());
			
			for (String s : gr_list) {
				ret.add(getValueByField(s));
			}
			
			return ret;
		}
	}

	private FlexyTupleFactory parent_tf;
	private FFields schema;
	private Map<String, Integer> sch_map = new HashMap<String, Integer>();
	int idx_start = 0;
	int idx_end = 0;
	
	public FlexyTupleFactory(FlexyTupleFactory parent_tf, FFields schema) {
		this.parent_tf = parent_tf;
		this.schema = schema;
		
		if (parent_tf != null) {
			idx_start = parent_tf.idx_end;
		}
		
		for (String s : schema.toList()) {
			sch_map.put(s, idx_end);
			idx_end++;
		}
		
		idx_end = idx_start + schema.toList().size();
	}

	public IFlexyTuple create(IFlexyTuple tup) {
		return create(tup.select(schema));
	}

	public IFlexyTuple create(IFlexyTuple parent, List<Object> values) {
		return new FlexyTupleImpl(parent, values);
	}

	public IFlexyTuple create(List<Object> values) {
		return new FlexyTupleImpl(null, values);
	}
	
	public static FlexyTupleFactory newFreshOutputFactory(FFields inputSchema) {
		return new FlexyTupleFactory(null, inputSchema);
	}
	
	public static FlexyTupleFactory newProjectionFactory(FFields inputFields) {
		return new FlexyTupleFactory(null, inputFields);
	}

	public static FlexyTupleFactory newOperationOutputFactory(
			FlexyTupleFactory parent_tf, FFields appendOutputFields) {
		return new FlexyTupleFactory(parent_tf, appendOutputFields);
	}
}
