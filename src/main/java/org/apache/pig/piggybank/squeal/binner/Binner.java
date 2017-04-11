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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.piggybank.squeal.flexy.components.IFlexyTuple;
import org.apache.pig.piggybank.squeal.flexy.components.IOutputCollector;
import org.apache.pig.piggybank.squeal.flexy.components.IRunContext;
import org.apache.pig.piggybank.squeal.flexy.model.FFields;

import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.generated.Grouping;
import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.serialization.KryoValuesDeserializer;
import backtype.storm.serialization.KryoValuesSerializer;
import backtype.storm.task.WorkerTopologyContext;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class Binner {
	
	// FIXME: Get rid of the Kryo stuff.  Use Hadoop's serialization directly.
	public static final String WRITE_THRESH_CONF = "flexy.binner.write.threshold";
	int _write_thresh = 64*1024;
	private KryoValuesSerializer _ser;
	private static final Log log = LogFactory.getLog(Binner.class);
	
	class OutCollector {
		Output out;
		Object aKey;
		
		OutCollector(Object aKey) {
			this.aKey = aKey;
			out = new Output(_write_thresh, -1);
		}
	}
	
	// Output bins for each downstream taskid.
	Map<Integer, OutCollector> bins = new HashMap<Integer, OutCollector>();	
	private List<Grouper> groupings = new ArrayList<Grouper>();
	
	private IOutputCollector collector;
	private String exposedName;
	private int taskId;
	
	static class Grouper { // implements CustomStreamGrouping {

		private Grouping gr;
		private CustomStreamGrouping wrapped;
		private List<Integer> targetTasks;
		private FFields gr_fields;
		private int num_tasks;
		Random r = new Random();

		public Grouper(Grouping gr) {
			this.gr = gr;
		}
		
		public void prepare(WorkerTopologyContext context,
				GlobalStreamId stream, List<Integer> targetTasks) {
			
			if (gr.is_set_fields()) {
				gr_fields = new FFields(gr.get_fields());
			} else if (gr.is_set_custom_serialized()) {
				wrapped = (CustomStreamGrouping) Utils.deserialize(gr.get_custom_serialized());
				wrapped.prepare(context, stream, targetTasks);
			}
			
			this.targetTasks = targetTasks;
			num_tasks = targetTasks.size();
		}

		public List<Integer> chooseTasks(int taskId, final IFlexyTuple tup) {
			if (gr.is_set_fields()) {
				return new ArrayList() {{ add(targetTasks.get(Math.abs(tup.select(gr_fields).hashCode()) % num_tasks)); }};
			} else if (gr.is_set_shuffle()) {
				return new ArrayList() {{ add(targetTasks.get(r.nextInt(num_tasks))); }};
			} else if (wrapped != null) {
				return wrapped.chooseTasks(taskId, tup.getValues());
			} else {
				throw new RuntimeException("Unknown grouping type: " + gr.getSetField());
			}
		}
		
	}
	
	public void prepare(IRunContext context,
			IOutputCollector collector, String exposedName) {
		this.collector = collector;
		this.exposedName = exposedName;
		taskId = context.getThisTaskId();
		
		// Determine the downstream subscribers.
		for (Entry<String, Grouping> ent : context.getStormTopologyContext().getThisTargets().get(exposedName).entrySet()) {
			Grouper gr = new Grouper(ent.getValue());
			gr.prepare(context.getStormTopologyContext(), new GlobalStreamId(context.getThisComponentId(), exposedName), 
					context.getStormTopologyContext().getComponentTasks(ent.getKey()));
			groupings.add(gr);
		}
		
		// Grab Kryo instances.
		_ser = new KryoValuesSerializer(context.getStormConf());
		
		// Pull any configuration overrides.
		Number conf_int = (Number) context.get(WRITE_THRESH_CONF);
		if (conf_int != null) _write_thresh = conf_int.intValue(); 
	}

	public void emit(IFlexyTuple tup, Object anchor) throws IOException {
		for (Grouper gr : groupings) {
			// Calculate the destinations.
			for (Integer dest : gr.chooseTasks(taskId, tup)) {				
				// Pull the current buffer.
				OutCollector curOut = bins.get(dest);
				if (curOut == null) {
					curOut = new OutCollector(tup.get(0));
					bins.put(dest, curOut);
				}

				// write the data out.
				_ser.serializeInto(tup.getValues(), curOut.out);

				// Determine if we need to flush this buffer
				if (curOut.out.position() > _write_thresh) {
					bins.remove(dest);
					_flush(curOut, anchor);
				}
			}
		}
	}

	private void _flush(OutCollector curOut, Object anchor) {
		// Emit curOut.
		collector.emit(exposedName, (Tuple) anchor, new Values(curOut.aKey, curOut.out.toBytes()));
	}
	
	public void flush(Object inputAnchor) {
		// Flush all the bins.
		for (Entry<Integer, OutCollector> ent : bins.entrySet()) {
			_flush(ent.getValue(), inputAnchor);
		}
		bins.clear();
	}

	public static class BinDecoder {
		private KryoValuesDeserializer _deser;

		public BinDecoder(IRunContext context) {
			
			_deser = new KryoValuesDeserializer(context.getStormConf());
		}
		
		public List<Object> decodeList(Input in) {
			if (in.position() >= in.limit()) {
				return null;
			}

			return _deser.deserializeFrom(in);
		}
	}
		
}
