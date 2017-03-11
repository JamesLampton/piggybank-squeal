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

package org.apache.pig.piggybank.squeal.flexy.oper;

import java.util.ArrayList;
import java.util.List;

import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.io.NullableTuple;
import org.apache.pig.piggybank.squeal.flexy.components.ICollector;
import org.apache.pig.piggybank.squeal.flexy.components.IFlexyTuple;
import org.apache.pig.piggybank.squeal.flexy.components.IFunction;
import org.apache.pig.piggybank.squeal.flexy.components.IRunContext;
import org.apache.pig.piggybank.squeal.flexy.model.FValues;

public class MakePigTuples implements IFunction {
	
	Integer POS = new Integer(1);
	Integer NEG = new Integer(-1);
	private TupleFactory tf;
	
	public void prepare(IRunContext context) {
		 tf = TupleFactory.getInstance();
	}
	
	public void execute(IFlexyTuple tuple, ICollector collector) {
		List<Object> ret_arr = new ArrayList<Object>(tuple.size());
		
		for (Object o : tuple.getValues()) {
			if (o instanceof byte[]) {
				ret_arr.add(new DataByteArray((byte[]) o));
			} else {
				ret_arr.add(o);
			}
		}
		
		collector.emit(new FValues(null, new NullableTuple(tf.newTupleNoCopy(ret_arr)), POS));
	}

	@Override
	public void cleanup() {
		
	}

}
