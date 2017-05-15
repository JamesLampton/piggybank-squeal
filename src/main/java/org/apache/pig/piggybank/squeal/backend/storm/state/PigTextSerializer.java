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

import java.io.IOException;
import java.io.ObjectStreamException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.pig.backend.hadoop.HDataType;
import org.apache.pig.builtin.PigStreaming;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.io.PigNullableWritable;
import org.apache.pig.piggybank.squeal.flexy.components.ISerializer;
import org.apache.pig.piggybank.squeal.flexy.model.FValues;

/*
 * Good for key serialization for States, but don't use to store the value because
 * this mechanism looses the schema information!
 */
public class PigTextSerializer implements ISerializer<List<Object>> {
	
	private PigStreaming ps;
	private TupleFactory tf;

	public PigTextSerializer() {
		ps = new PigStreaming();
		tf = TupleFactory.getInstance();
	}
	
	@Override
	public byte[] serialize(List<Object> objl) {
		try {
			ArrayList<Object> arr = new ArrayList<Object>(objl.size());
			for (Object o : objl) {
				arr.add(((PigNullableWritable) o).getValueAsPigType());
			}
			return ps.serialize(tf.newTupleNoCopy(arr));
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public List<Object> deserialize(byte[] b) {
		try {
			FValues v = new FValues();			
			for (Object o : ps.deserialize(b).getAll()) {
				v.add(HDataType.getWritableComparableTypes(((Tuple)o).get(0), DataType.findType(o)));
			}
			return v;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}		
	}

	Object writeReplace() throws ObjectStreamException {
		return new SerializedForm();
	}
	
	static class SerializedForm implements Serializable {
		Object readResolve() throws ObjectStreamException {
			return new PigTextSerializer();
		}
	}
}
