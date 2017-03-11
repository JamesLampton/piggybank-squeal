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

package org.apache.pig.piggybank.squeal.flexy.oper;

import java.net.InetAddress;
import java.util.List;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PigLogger;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PigProgressable;
import org.apache.pig.piggybank.squeal.backend.storm.MonkeyPatch;
import org.apache.pig.piggybank.squeal.flexy.components.ICollector;
import org.apache.pig.piggybank.squeal.flexy.components.IFlexyTuple;
import org.apache.pig.piggybank.squeal.flexy.components.IFunction;
import org.apache.pig.piggybank.squeal.flexy.components.IRunContext;
import org.apache.pig.piggybank.squeal.metrics.IMetricsTransport;
import org.apache.pig.piggybank.squeal.metrics.MetricsTransportFactory;
import org.apache.pig.data.SchemaTupleBackend;
import org.apache.pig.impl.PigContext;

public abstract class FlexyBaseFunction implements IFunction {

	protected PigContext pc;
	protected IMetricsTransport mt;
	private Random r;
	private int taskId;
	private int taskIdx;
	private String compId;
	private String stormId;
	private String hostname;
	
	public FlexyBaseFunction() {
		
	}
	
	public FlexyBaseFunction(PigContext pc) {
		this.pc = pc;
	}
	
	// FIXME: Make a default?
	public abstract String getMetricsAnnotation();
	
	private static final Log log = LogFactory.getLog(FlexyBaseFunction.class);

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
	
	public void	prepare(IRunContext context) {		
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
		
		// Pull the component name and any other information from the conf.
		try {
			taskId = context.getThisTaskId();
			taskIdx = context.getThisTaskIndex();
			compId = context.getThisComponentId();
			stormId = context.getStormId();
			hostname = InetAddress.getLocalHost().getHostName();
		} catch (Exception e) {
			// Leave things as unknown on error.
		}

		// Send an initial declare message.
		if (mt != null) {
			_send(null, null, new Object[] {"DECL_FUNC", hostname, stormId, taskId, taskIdx, compId, getMetricsAnnotation()});
		}
	}
	
	void _send(Object[] appends, Object[] appends2, Object[] msg) {
		StringBuilder sb = new StringBuilder();

		sb.append(System.currentTimeMillis());
		sb.append("\t");
		sb.append(taskId);

		for (Object o : msg) {
			sb.append("\t");
			sb.append(o == null ? "" : o.toString().replace("\n", " ").replace("\t", " "));
		}
		if (appends != null) {
			for (Object o : appends) {
				sb.append("\t");
				sb.append(o == null ? "" : o.toString().replace("\n", " ").replace("\t", " "));
			}
		}
		if (appends2 != null) {
			for (Object o : appends2) {
				sb.append("\t");
				sb.append(o == null ? "" : o.toString().replace("\n", " ").replace("\t", " "));
			}
		}
		sb.append("\n");

		mt.send(sb.toString().getBytes());
	}
	
	class MetricsCollector implements ICollector {

		private ICollector wrapped;
		private long start_ts;
		int pos_count = 0;
		int neg_count = 0;
		int unkn_count = 0;
		private Object[] appends;

		public MetricsCollector(ICollector collector, Object[] appends) {
			start_ts = System.nanoTime();
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
			long stop_ts = System.nanoTime();

			send(appends2, getMetricsAnnotation(), stop_ts-start_ts, pos_count, neg_count, unkn_count);
		}
		
		void send(Object[] appends2, Object... msg) {
			_send(appends, appends2, msg);
		}
	}
	
	protected ICollector doMetricsStart(ICollector collector, Object... appends) {
		if (mt == null || !mt.shouldSample()) {
			return collector;
		}
		
		// Wrap this collector in a metrics-tracking collector.
		collector = new MetricsCollector(collector, appends);
		
		return collector;
	}
	
	protected void doMetricsStop(ICollector collector, Object... appends2) {
		if (collector instanceof MetricsCollector) {
			((MetricsCollector)collector).collectMetrics(appends2);
		}
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}
}
