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

package org.apache.pig.piggybank.squeal.backend.storm.io;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.pig.backend.hadoop.HDataType;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.io.PigNullableWritable;
import org.apache.pig.piggybank.squeal.backend.storm.state.StateWrapper;
import org.apache.pig.piggybank.squeal.flexy.components.IMapState;
import org.apache.pig.piggybank.squeal.flexy.components.IStateFactory;
import org.apache.pig.piggybank.squeal.flexy.components.impl.FakeRunContext;

import backtype.storm.tuple.Values;

public class StateStore extends PigStorage implements ISignStore {

	private String jsonOpts;
	private AtomicInteger sign;
	private IMapState s;

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
			IStateFactory stateFactory = new StateWrapper(jsonOpts).getStateFactory();
			s = (IMapState) stateFactory.makeState(new FakeRunContext());
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
