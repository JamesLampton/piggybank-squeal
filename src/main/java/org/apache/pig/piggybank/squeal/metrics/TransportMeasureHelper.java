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

package org.apache.pig.piggybank.squeal.metrics;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.pig.piggybank.squeal.MonkeyPatch;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import storm.trident.Stream;
import storm.trident.fluent.ChainedFullAggregatorDeclarer;
import storm.trident.fluent.ChainedPartitionAggregatorDeclarer;
import storm.trident.operation.Aggregator;
import storm.trident.operation.CombinerAggregator;
import storm.trident.operation.Filter;
import storm.trident.operation.Function;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.operation.impl.Result;
import storm.trident.state.map.MapCombinerAggStateUpdater;
import storm.trident.state.map.MapState;
import storm.trident.tuple.TridentTuple;

public class TransportMeasureHelper {
	
	static final Fields instrumentFields = new Fields("source_id__", "source_hashcode__", "timestamp__", "recordcount__");
	static ConcurrentMap<String, PreAggregator> preMap = new ConcurrentHashMap<String, PreAggregator>();
	
	static void send(IMetricsTransport mt, Object... msg) {
		StringBuilder sb = new StringBuilder();

		sb.append(System.currentTimeMillis());

		for (Object o : msg) {
			sb.append("\t");
			sb.append(o == null ? "" : o.toString());
		}
		sb.append("\n");

		mt.send(sb.toString().getBytes());
	}

	public static class AppendSourceAndTime implements Function {
		private int taskId;
		protected IMetricsTransport mt;
		private String name;
		
		public AppendSourceAndTime(String name) {
			this.name = name;
		}
		
		@Override
		public void prepare(Map conf, TridentOperationContext context) {
			mt = MetricsTransportFactory.getInstance(conf, ClassLoader.getSystemClassLoader());
			taskId = MonkeyPatch.getTopologyContext(context).getThisTaskId();
			
			send(mt, "DECLARE_APPEND_SOURCE", taskId, mt.getSampleRate(), name, hashCode());
		}

		@Override
		public void cleanup() {		
		}

		@Override
		public void execute(TridentTuple tuple, TridentCollector collector) {
			long ts = 0;
			if (mt.shouldSample()) {
				ts = System.currentTimeMillis();
			}
			collector.emit(new Values(taskId, hashCode(), ts, 1));
		}
		
	}
	
	public static Stream instrument(Stream output, String name) {
		return output.each(new AppendSourceAndTime(name), instrumentFields);
	}
	
	public static class RecordTimeFilter implements Filter {

		private IMetricsTransport mt;
		private int taskId;
		private String name;
		
		public RecordTimeFilter(String name) {
			this.name = name;
		}

		@Override
		public void prepare(Map conf, TridentOperationContext context) {
			mt = MetricsTransportFactory.getInstance(conf, ClassLoader.getSystemClassLoader());
			taskId = MonkeyPatch.getTopologyContext(context).getThisTaskId();
			send(mt, "DECLARE_RECV_SOURCE", taskId, name, hashCode());
		}

		@Override
		public void cleanup() {
			
		}

		@Override
		public boolean isKeep(TridentTuple tuple) {
			Integer source_id = tuple.getInteger(0);
			Integer source_hashcode = tuple.getInteger(1);
			long start_ts = tuple.getLong(2);
			Integer count = tuple.getInteger(3);
			
			if (start_ts != 0) {
				// Note the receive.
				send(mt, "TR_RECV", taskId, hashCode(), source_id, source_hashcode, start_ts, count);
			}
			
			return true;
		}
		
	}
	
	public static Stream extractAndRecord(Stream input, String name) {
		input = input.each(instrumentFields, new RecordTimeFilter(name));
		List<String> cur_fields = input.getOutputFields().toList();
		
		return input.project(new Fields(cur_fields.subList(0, cur_fields.size() - instrumentFields.size())));
	}
	
	static class PreAggregator implements Aggregator<Result> {
		
		private IMetricsTransport mt;
		private String agg_uuid;
		private long start_ns;
		private final List<Object> VAL = new Values(0);
		
		public PreAggregator(String agg_uuid) {
			this.agg_uuid = agg_uuid;
		}

		@Override
		public void prepare(Map conf, TridentOperationContext context) {
			// Register the current object with the static listing.
			mt = MetricsTransportFactory.getInstance(conf, ClassLoader.getSystemClassLoader());
			int taskId = MonkeyPatch.getTopologyContext(context).getThisTaskId();
			preMap.put(agg_uuid + "--" + taskId, this);			
		}

		@Override
		public void cleanup() {
			
		}

		@Override
		public Result init(Object batchId, TridentCollector collector) {
			return null;
		}

		void doSample() {
			start_ns = 0;
			if (mt != null && mt.shouldSample()) {
				start_ns = System.nanoTime();				
			}
		}
		
		long checkSample() {
			if (start_ns == 0) {
				return 0;
			}
			
			long ret = System.nanoTime() - start_ns;
			start_ns = 0;
			return ret;
		}
		
		@Override
		public void aggregate(Result val, TridentTuple tuple,
				TridentCollector collector) {
			doSample();
		}

		@Override
		public void complete(Result val, TridentCollector collector) {
			doSample();
			collector.emit(VAL);
		}
	}

	static class Stage1Aggregator implements Aggregator<Result> {
		private String mark;
		private String pre_mark;
		private String name;
		private String step;
		private String agg_uuid;

		public Stage1Aggregator(String name, String agg_uuid) {
			this(name, agg_uuid, "S1");
		}
		
		public Stage1Aggregator(String name, String agg_uuid, String step) {
			this.name = name;
			this.step = step;
			this.mark = "TR_" + step + "_RECV";
			this.pre_mark = "AGG_" + step;
			this.agg_uuid = agg_uuid;
		}
		
		protected IMetricsTransport mt;
		protected int taskId;
		private PreAggregator paired;

		@Override
		public void prepare(Map conf, TridentOperationContext context) {
			mt = MetricsTransportFactory.getInstance(conf, ClassLoader.getSystemClassLoader());
			taskId = MonkeyPatch.getTopologyContext(context).getThisTaskId();
			Object sr = conf.get("topology.stats.sample.rate");
			send(mt, "DECLARE_AGG_SOURCE", taskId, mt.getSampleRate(), name, hashCode(), step);
			
			// Get the paired PreAggregator.
			paired = preMap.get(agg_uuid + "--" + taskId);
		}

		@Override
		public void cleanup() {
			
		}

		static final Integer ZERO = 0;
		@Override
		public Result init(Object batchId, TridentCollector collector) {
			Result val = new Result();
			val.obj = ZERO;
			return val;
		}

		@Override
		public void aggregate(Result val, TridentTuple tuple,
				TridentCollector collector) {
			// Check to see if our pair ran.
			long paired_ts = paired.checkSample();
			if (paired_ts != 0) {
				send(mt, pre_mark, taskId, hashCode(), 0, paired_ts);
			}
			
			Integer source_id = tuple.getInteger(0);
			Integer source_hashcode = tuple.getInteger(1);
			long start_ts = tuple.getLong(2);
			Integer count = tuple.getInteger(3);

			val.obj = count + (Integer) val.obj;
			
			if (start_ts != 0) {
				// Note the receive.
				send(mt, mark, taskId, hashCode(), source_id, source_hashcode, start_ts, count);
			}
		}

		@Override
		public void complete(Result val, TridentCollector collector) {
			// Check to see if our pair ran.
			long paired_ts = paired.checkSample();
			if (paired_ts != 0) {
				send(mt, pre_mark, taskId, hashCode(), 1, paired_ts);
			}
			
			long ts = 0;
			if (mt.shouldSample()) {
				ts = System.currentTimeMillis();
			}
			collector.emit(new Values(taskId, hashCode(), ts, val.obj));
		}

	}
		
	public static ChainedPartitionAggregatorDeclarer instrument(ChainedPartitionAggregatorDeclarer agg_chain, String name, String agg_uuid) {
		return agg_chain.partitionAggregate(instrumentFields, new Stage1Aggregator(name, agg_uuid), instrumentFields);

	}

	public static ChainedFullAggregatorDeclarer instrument(
			ChainedFullAggregatorDeclarer agg_chain, String name, String agg_uuid) {
		return agg_chain.aggregate(instrumentFields, new Stage1Aggregator(name, agg_uuid, "S2"), instrumentFields);
	}

	public static ChainedPartitionAggregatorDeclarer instrument_pre(
			ChainedPartitionAggregatorDeclarer part_agg_chain, String name, String agg_uuid) {
		return part_agg_chain.partitionAggregate(new PreAggregator(agg_uuid), new Fields("preAgg"));
	}

	public static ChainedFullAggregatorDeclarer instrument_pre(
			ChainedFullAggregatorDeclarer agg_chain, String name,
			String agg_uuid) {
		return agg_chain.aggregate(new PreAggregator(agg_uuid), new Fields("preAgg2"));
	}

}
