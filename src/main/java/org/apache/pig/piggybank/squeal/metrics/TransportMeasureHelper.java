package org.apache.pig.piggybank.squeal.metrics;

import java.util.List;
import java.util.Map;
import java.util.Random;

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
	
	static final Fields instrumentFields = new Fields("source_id__", "timestamp__", "recordcount__");
	
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
		private Random r;
		private double sample_rate = 0.05;
		
		@Override
		public void prepare(Map conf, TridentOperationContext context) {
			mt = MetricsTransportFactory.getInstance(conf, ClassLoader.getSystemClassLoader());
			r = new Random();
			taskId = MonkeyPatch.getTopologyContext(context).getThisTaskId();
			Object sr = conf.get("topology.stats.sample.rate");
			if (sr != null) {
				sample_rate = Double.parseDouble(sr.toString());
			}
			
			send(mt, "DECLARE_APPEND_SOURCE", taskId, sample_rate);
		}

		@Override
		public void cleanup() {		
		}

		@Override
		public void execute(TridentTuple tuple, TridentCollector collector) {
			long ts = 0;
			if (r.nextDouble() <= sample_rate) {
				ts = System.currentTimeMillis();
			}
			collector.emit(new Values(taskId, ts, 1));
		}
		
	}
	
	public static Stream instrument(Stream output) {
		return output.each(new AppendSourceAndTime(), instrumentFields);
	}
	
	public static class RecordTimeFilter implements Filter {

		private IMetricsTransport mt;
		private int taskId;

		@Override
		public void prepare(Map conf, TridentOperationContext context) {
			mt = MetricsTransportFactory.getInstance(conf, ClassLoader.getSystemClassLoader());
			taskId = MonkeyPatch.getTopologyContext(context).getThisTaskId();
		}

		@Override
		public void cleanup() {
			
		}

		@Override
		public boolean isKeep(TridentTuple tuple) {
			Integer source_id = tuple.getInteger(0);
			long start_ts = tuple.getLong(1);
			Integer count = tuple.getInteger(2);
			
			if (start_ts != 0) {
				// Note the receive.
				send(mt, "TR_RECV", taskId, source_id, start_ts, count);
			}
			
			return true;
		}
		
	}
	
	public static Stream extractAndRecord(Stream input) {
		input = input.each(instrumentFields, new RecordTimeFilter());
		List<String> cur_fields = input.getOutputFields().toList();
		
		return input.project(new Fields(cur_fields.subList(0, cur_fields.size() - instrumentFields.size())));
	}

	static class Stage1Aggregator implements Aggregator<Result> {

		private String mark;

		public Stage1Aggregator() {
			this("S1");
		}
		
		public Stage1Aggregator(String step) {
			this.mark = "TR_" + step + "_RECV";
		}
		
		protected IMetricsTransport mt;
		private Random r;
		protected int taskId;
		private double sample_rate = 0.05;

		@Override
		public void prepare(Map conf, TridentOperationContext context) {
			mt = MetricsTransportFactory.getInstance(conf, ClassLoader.getSystemClassLoader());
			r = new Random();
			taskId = MonkeyPatch.getTopologyContext(context).getThisTaskId();
			Object sr = conf.get("topology.stats.sample.rate");
			if (sr != null) {
				sample_rate  = Double.parseDouble(sr.toString());
			}
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
			Integer source_id = tuple.getInteger(0);
			long start_ts = tuple.getLong(1);
			Integer count = tuple.getInteger(2);

			val.obj = count + (Integer) val.obj;
			
			if (start_ts != 0) {
				// Note the receive.
				send(mt, mark, taskId, source_id, start_ts, count);
			}
		}

		@Override
		public void complete(Result val, TridentCollector collector) {
			long ts = 0;
			if (r.nextDouble() <= sample_rate) {
				ts = System.currentTimeMillis();
			}
			collector.emit(new Values(taskId, ts, val.obj));
		}

	}
		
	public static ChainedPartitionAggregatorDeclarer instrument(ChainedPartitionAggregatorDeclarer agg_chain) {
		return agg_chain.partitionAggregate(instrumentFields, new Stage1Aggregator(), instrumentFields);

	}

	public static ChainedFullAggregatorDeclarer instrument(
			ChainedFullAggregatorDeclarer agg_chain) {
		return agg_chain.aggregate(instrumentFields, new Stage1Aggregator("S2"), instrumentFields);
	}

}
