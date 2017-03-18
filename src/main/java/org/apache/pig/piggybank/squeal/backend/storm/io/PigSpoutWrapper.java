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

import org.apache.hadoop.mapreduce.Job;
import org.apache.pig.ResourceSchema;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.io.NullableTuple;
import org.apache.pig.impl.util.StorageUtil;
import org.apache.pig.piggybank.squeal.flexy.components.ICollector;
import org.apache.pig.piggybank.squeal.flexy.components.IFlexyTuple;
import org.apache.pig.piggybank.squeal.flexy.components.IFunction;
import org.apache.pig.piggybank.squeal.flexy.components.IRunContext;

import backtype.storm.tuple.Values;

public class PigSpoutWrapper extends SpoutWrapper {

	public PigSpoutWrapper(String spoutClass) {
		super(spoutClass, null);
	}
	
	public PigSpoutWrapper(String spoutClass, String jsonArgs) {
		super(spoutClass, jsonArgs, null);
	}
	
	public PigSpoutWrapper(String spoutClass, String jsonArgs, String parallelismHint) {
		super(spoutClass, jsonArgs, parallelismHint);
	}
	
	@Override
	public ResourceSchema getSchema(String location, Job job)
			throws IOException {
		return null;
	}
	
	public Class<? extends IFunction> getTupleConverter() {
		return MakePigTuples.class;
	}
	
	static public class MakePigTuples implements IFunction {
		Integer POS = new Integer(1);
		Integer NEG = new Integer(-1);
		private TupleFactory tf;
		
		@Override
		public void prepare(IRunContext context) {
			 tf = TupleFactory.getInstance();
		}
			
		@Override
		public void execute(IFlexyTuple tuple, ICollector collector) {
			byte[] buf;
			try {
				buf = DataType.toBytes(tuple.get(0));
			} catch (ExecException e) {
				throw new RuntimeException(e);
			}
			
			Tuple t = StorageUtil.bytesToTuple(buf, 0, buf.length, (byte) '\t');			
			
			collector.emit(new Values(null, new NullableTuple(t), POS));
		}

		@Override
		public void cleanup() {
			
		}

	}
}
