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

package org.apache.pig.piggybank.squeal.flexy.executors;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class FlexyTracer {
	// Initialized upon creation.
	long c0_createTime = 0;
	// Written out upon serialization.
	long c0_encTime = 0;
	// Marked on deserialization
	long c1_decTime = 0;
	long c1_markTime = 0;
	
	public FlexyTracer() {
		this(System.currentTimeMillis(), 0);
	}
	
	private FlexyTracer(long c0_createTime, long c0_encTime) {
		this.c0_createTime = c0_createTime;
		this.c0_encTime = c0_encTime;
	}
	
	public long mark() {
		// Mark when this tracer is acted upon.
		if (c1_markTime == 0) {
			c1_markTime = System.currentTimeMillis();
		}
		return c1_markTime;
	}
	
	public long getTotalDelay() {
		return mark() - c0_createTime;
	}
	
	public long getRecvQueueTime() {
		return mark() - c1_decTime;
	}
	
	public long getEmitQueueTime() {
		return c0_encTime - c0_createTime;
	}
	
	public long getNetworkTime() {
		return c1_decTime - c0_encTime;
	}
	
	static public class TracerKryoSerializer extends Serializer<FlexyTracer> {

		@Override
		public FlexyTracer read(Kryo kryo, Input input, Class<FlexyTracer> w) {
			long createTime = input.readLong();
			long encTime = input.readLong();
			
			FlexyTracer ret = new FlexyTracer(createTime, encTime);
			ret.c1_decTime = System.currentTimeMillis();
			
			return ret;
		}

		@Override
		public void write(Kryo kryo, Output output, FlexyTracer tracer) {
			output.writeLong(tracer.c0_createTime);
			output.writeLong(System.currentTimeMillis());
		}
	}
}
