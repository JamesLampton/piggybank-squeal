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

package org.apache.pig.piggybank.squeal.backend.storm.oper;

import java.util.ArrayList;
import java.util.List;

import org.apache.pig.data.DataByteArray;
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
