package org.apache.pig.piggybank.squeal.backend.storm.oper;

import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PigLogger;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PigProgressable;
import org.apache.pig.piggybank.squeal.MonkeyPatch;
import org.apache.pig.piggybank.squeal.backend.storm.Main;
import org.apache.pig.piggybank.squeal.metrics.IMetricsTransport;
import org.apache.pig.piggybank.squeal.metrics.MetricsTransportFactory;
import org.apache.pig.data.SchemaTupleBackend;
import org.apache.pig.impl.PigContext;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;

public abstract class StormBaseFunction extends BaseFunction {

	protected PigContext pc;
	protected IMetricsTransport mt;
	double sample_rate = 0.1;
	private Random r;
	static public final String SAMPLE_RATE_KEY = "pig.streaming.metrics.sample.rate";

	public StormBaseFunction(PigContext pc) {
		this.pc = pc;
		
		sample_rate = Double.parseDouble(pc.getProperties().getProperty(SAMPLE_RATE_KEY, "0.1"));
	}
	
	public abstract String getMetricsAnnotation();
	
	private static final Log log = LogFactory.getLog(StormBaseFunction.class);

	class DummyProgress implements PigProgressable {
		@Override
		public void progress() {
			
		}
		@Override
		public void progress(String msg) {
			
		}
	}
	
	class DummyLogger implements PigLogger {
		@Override
		public void warn(Object o, String msg, Enum warningEnum) {
			System.err.println(o.toString() + " " + msg);
//			log.warn(o.toString() + " " + msg);
		}
	}
	
	public void	prepare(Map conf, TridentOperationContext context) {
		super.prepare(conf, context);
		
		try {
			MonkeyPatch.PigContextRefreshEngine(pc);
			pc.connect();
			
			// FIXME: Try an empty configuration.
			SchemaTupleBackend.initialize(new Configuration(), pc);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		
		PhysicalOperator.setReporter(new DummyProgress());
		PhysicalOperator.setPigLogger(new DummyLogger());
		
		// Pull a metrics transport if configured.
		mt = MetricsTransportFactory.getInstance(pc);
		r = new Random();
	}
	
	class MetricsCollector implements TridentCollector {

		private TridentCollector wrapped;
		private long start_ts;
		int pos_count = 0;
		int neg_count = 0;
		int unkn_count = 0;
		private Object[] appends;

		public MetricsCollector(TridentCollector collector, Object[] appends) {
			start_ts = System.currentTimeMillis();
			this.appends = appends;
			wrapped = collector;
		}
		
		@Override
		public void emit(List<Object> values) {
			// Count the record types.
			Object tive = values.get(values.size()-1);
			if (tive instanceof Integer) {
				int signum = Integer.signum((int) tive);
				if (signum > 0) {
					pos_count += 1;
				} else if (signum < 0) {
					neg_count += 1;
				} else {
					unkn_count += 1;
				}
			}
			wrapped.emit(values);
		}

		@Override
		public void reportError(Throwable t) {
			wrapped.reportError(t);
		}
		
		public void collectMetrics(Object[] appends2) {
			long stop_ts = System.currentTimeMillis();

			send(appends2, getMetricsAnnotation(), stop_ts-start_ts, pos_count, neg_count, unkn_count);
		}
		
		void send(Object[] appends2, Object... msg) {
			StringBuilder sb = new StringBuilder();

			sb.append(System.currentTimeMillis());

			for (Object o : msg) {
				sb.append("\t");
				sb.append(o == null ? "" : o.toString().replace("\n", " ").replace("\t", " "));
			}
			for (Object o : appends) {
				sb.append("\t");
				sb.append(o == null ? "" : o.toString().replace("\n", " ").replace("\t", " "));
			}
			for (Object o : appends2) {
				sb.append("\t");
				sb.append(o == null ? "" : o.toString().replace("\n", " ").replace("\t", " "));
			}
			sb.append("\n");

			mt.send(sb.toString().getBytes());
		}
	}
	
	protected TridentCollector doMetricsStart(TridentCollector collector, Object... appends) {
		if (mt == null || r.nextDouble() > sample_rate) {
			return collector;
		}
		
		// Wrap this collector in a metrics-tracking collector.
		collector = new MetricsCollector(collector, appends);
		
		return collector;
	}
	
	protected void doMetricsStop(TridentCollector collector, Object... appends2) {
		if (collector instanceof MetricsCollector) {
			((MetricsCollector)collector).collectMetrics(appends2);
		}
	}
}
