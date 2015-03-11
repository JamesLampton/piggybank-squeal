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

import java.util.Map;
import java.util.Random;

public abstract class AbstractMetricsTransport implements IMetricsTransport {
	private double sample_rate = 0.05;
	private Random rand;
	static public final String SAMPLE_RATE_KEY = "pig.streaming.metrics.sample.rate";

	public void initialize(Map props) {
		rand = new Random();
		Object sr = props.get(SAMPLE_RATE_KEY);
		if (sr != null) {
			sample_rate  = Double.parseDouble(sr.toString());
		}
	}
	
	@Override
	public void send(Object... objects) {
		StringBuilder sb = new StringBuilder();
		
		sb.append(System.currentTimeMillis());
		
		for (Object o : objects) {
			sb.append("\t");
			sb.append(o == null ? "" : o.toString());
		}
		
		send(sb.toString().getBytes());
	}

	@Override
	public abstract void send(byte[] buf);
	

	@Override
	public boolean shouldSample() {
		if (rand.nextDouble() > sample_rate) {
			return false;
		}
		return true;
	}
	
	@Override
	public double getSampleRate() {
		return sample_rate;
	}
}
