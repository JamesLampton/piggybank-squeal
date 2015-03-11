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

import org.apache.hadoop.io.Writable;
import org.apache.pig.piggybank.squeal.backend.storm.state.PigSerializer;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class WritableKryoSerializer extends Serializer<Writable> {
	
	PigSerializer ps = new PigSerializer();

	@Override
	public Writable read(Kryo kryo, Input input, Class<Writable> w) {
		int len = input.readInt();
		byte[] buf = input.readBytes(len);
		return (Writable) ps.deserialize(buf);
	}

	@Override
	public void write(Kryo kryo, Output output, Writable w) {
		byte[] buf = ps.serialize(w);
		output.writeInt(buf.length);
		output.write(buf);
	}

}
