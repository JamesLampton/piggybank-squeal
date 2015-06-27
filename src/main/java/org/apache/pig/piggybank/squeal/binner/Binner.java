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

package org.apache.pig.piggybank.squeal.binner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import storm.trident.tuple.TridentTuple;
import backtype.storm.generated.Grouping;
import backtype.storm.serialization.KryoValuesDeserializer;
import backtype.storm.serialization.KryoValuesSerializer;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;

public class Binner {
	int _write_thresh = 64*1024;
	private KryoValuesSerializer _ser;
	private KryoValuesDeserializer _deser;
	
	// Output bins for each downstream taskid.
	Map<Integer, Output> bins;	
	private ArrayList<Grouping> groupings;
	
	private OutputCollector collector;
	private String exposedName;
	
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector, String exposedName) {
		this.collector = collector;
		this.exposedName = exposedName;
		
		// Determine the downstream subscribers.
		groupings = new ArrayList<Grouping>(context.getThisTargets().get(exposedName).values());
		
		// Grab Kryo instances.
		_ser = new KryoValuesSerializer(stormConf);
		_deser = new KryoValuesDeserializer(stormConf);
	}

	public void emit(TridentTuple tup) throws IOException {
		for (Grouping gr : groupings) {
			// Calculate the destinations.
//			for (Integer dest : gr.) {	
			int dest = 0;
			
			// Pull the current buffer.
			Output curOut = bins.get(dest);
			if (curOut == null) {
				curOut = new Output();
			}
			
			// write the data out.
			_ser.serializeInto(tup, curOut);
			
			// Determine if we need to flush this buffer
			if (curOut.position() > _write_thresh) {
				bins.remove(dest);
				_flush(dest, curOut);
			}
			
//			}
		}
		
		// TODO Auto-generated method stub
		// FIXME: Emit directly for now.
		collector.emit(exposedName, tup);
	}

	private void _flush(int dest, Output curOut) {
		// TODO -- emit curOut.		
	}
	
	public void flush() {
		// TODO -- flush all bins.
	}

	public List<Object> getList(Input in) {
		if (in.eof()) {
			return null;
		}
		
		return _deser.deserializeFrom(in);
	}
		
}
