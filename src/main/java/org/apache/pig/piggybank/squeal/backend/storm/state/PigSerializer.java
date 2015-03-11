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

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import org.apache.hadoop.io.AbstractMapWritable;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.join.TupleWritable;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.HDataType;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.io.NullableBag;
import org.apache.pig.impl.io.NullableBooleanWritable;
import org.apache.pig.impl.io.NullableBytesWritable;
import org.apache.pig.impl.io.NullableDoubleWritable;
import org.apache.pig.impl.io.NullableFloatWritable;
import org.apache.pig.impl.io.NullableIntWritable;
import org.apache.pig.impl.io.NullableLongWritable;
import org.apache.pig.impl.io.NullableText;
import org.apache.pig.impl.io.NullableTuple;
import org.apache.pig.impl.io.PigNullableWritable;

import backtype.storm.tuple.Values;

import storm.trident.state.Serializer;

public class PigSerializer implements Serializer {
	
	@Override
	public byte[] serialize(Object o) {
		
		DataOutputBuffer dbuf = new DataOutputBuffer();
		
		try {
			if (o instanceof Values) {
				Values objl = (Values) o;
				dbuf.writeByte(0);
				// Write out the length.
				dbuf.writeInt(objl.size());
				
				// First, write the type.
				for (int i = 0; i < objl.size(); i++) {
					PigNullableWritable pnw = (PigNullableWritable) objl.get(i);
					dbuf.writeByte(HDataType.findTypeFromNullableWritable(pnw));
					pnw.write(dbuf);
				}				
			} else if (o instanceof Writable) {
				dbuf.writeByte(1);
				dbuf.writeUTF(o.getClass().getName());
				((Writable) o).write(dbuf);
			} else {
				throw new RuntimeException("Unexpected type: " + o.getClass());
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		
		return dbuf.getData();
	}

	@Override
	public Object deserialize(byte[] b) {
		
		ByteArrayInputStream bais = new ByteArrayInputStream(b);
		DataInputStream dis = new DataInputStream(bais);
		
		try {
			byte write_type = dis.readByte();
			Object ret = null;
			if (write_type == 0) {
				// Read the length.
				int values_size = dis.readInt();
				Values arr = new Values();
				
				for (int i = 0; i < values_size; i++) {
					// First read the type
					byte t = dis.readByte();
					// Get a new instance of the appropriate object.
					PigNullableWritable pnw = HDataType.getWritableComparableTypes(t).getClass().newInstance();
					pnw.readFields(dis);
					arr.add(pnw);
				}
				ret = arr;
			} else if (write_type == 1) {
				String cls = dis.readUTF();
				
				Writable c = (Writable) Class.forName(cls).newInstance();
				c.readFields(dis);
				ret = c;
			}
			
			return ret;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}				
	}

}
