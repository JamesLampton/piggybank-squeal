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

import java.util.Iterator;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POPackage;
import org.apache.pig.piggybank.squeal.flexy.components.ICombinerAggregator;
import org.apache.pig.piggybank.squeal.flexy.components.IFlexyTuple;
import org.apache.pig.piggybank.squeal.flexy.components.IMapState;
import org.apache.pig.piggybank.squeal.flexy.components.IStateFactory;
import org.apache.pig.piggybank.squeal.flexy.components.impl.FakeRunContext;
import org.apache.pig.piggybank.squeal.flexy.components.impl.FlexyTupleFactory;
import org.apache.pig.piggybank.squeal.flexy.model.FFields;
import org.apache.pig.piggybank.squeal.flexy.model.FValues;
import org.apache.pig.piggybank.squeal.flexy.oper.CombineWrapper;
import org.apache.pig.piggybank.squeal.flexy.oper.BasicPersist;
import org.apache.pig.piggybank.squeal.flexy.oper.WindowCombinePersist;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.io.NullableTuple;
import org.apache.pig.impl.io.PigNullableWritable;
import org.apache.pig.impl.plan.OperatorKey;

public class StatePack extends POPackage {

	private IStateFactory stateFactory;
	boolean initialized = false;
	IMapState s;
	ICombinerAggregator agg;
	FlexyTupleFactory tFactory;
	private String windowOpts;
	private TupleFactory tf;
	private final static Integer POS = 1;
	
	public StatePack(OperatorKey k, IStateFactory stateFactory, String windowOpts) {
		super(k);
		this.stateFactory = stateFactory;
		this.windowOpts = windowOpts;
	}
	
	@Override
	public void attachInput(PigNullableWritable k, Iterator<NullableTuple> inp) {
		if (initialized == false) {
			initialized = true;
			s = (IMapState) stateFactory.makeState(new FakeRunContext());
			
			if (windowOpts == null) {
				agg = new CombineWrapper(new BasicPersist());
			} else {
				agg = new CombineWrapper(new WindowCombinePersist(windowOpts));
			}
			tFactory = FlexyTupleFactory.newFreshOutputFactory(new FFields("k", "v", "s"));
			tf = TupleFactory.getInstance();
			
			System.out.println("TridentStatePack.attachInput initialized state: " + stateFactory + " agg: " + agg + " windowOpts: " + windowOpts);
			
		}

		// Aggregate the values.
		Object state = null;
		while (inp.hasNext()) {
			// Need to copy this thing because the reference is reused.
			NullableTuple ref = inp.next();
			Tuple tup = (Tuple) ref.getValueAsPigType();
			NullableTuple t = new NullableTuple(tf.newTuple(tup.getAll()));
			t.setIndex(ref.getIndex());
			
			// Create a tuple.
			IFlexyTuple tuple = tFactory.create(new FValues(k, t, POS));
			
			// Initialize the current tuple t.
			Object t_init = agg.init(tuple);
			
//			System.out.println("k: " + k + " t: " + t + " t_init: " + t_init + " state_pre: " + state);
			
			// And combine
			if (state == null) {
				state = t_init;
			} else {
				state = agg.combine(state, t_init);
			}
//			System.out.println("k: " + k + " t: " + t + " t_init: " + t_init + " state_post: " + state);
		}
		
		// Stash it out to the state.
//		System.out.println("Writing: " + k);
//		s.beginCommit(new Long(0));
		s.multiPut(new FValues(new FValues(k)), new FValues(state));
//		s.commit(new Long(0));
		
//		System.out.println("TridentStatePack.attachInput called -- State: " + s);
	}
	
	@Override
	public Result getNextTuple() throws ExecException {
		// All the trickery is in attach input.
		Result res = new Result();
		res.returnStatus = POStatus.STATUS_EOP;
		return res;
	}
}
