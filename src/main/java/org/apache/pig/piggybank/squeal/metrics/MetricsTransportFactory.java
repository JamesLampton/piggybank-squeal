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
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.pig.impl.PigContext;

public class MetricsTransportFactory {
	public static final String METRICS_TRANSPORT_KEY = "pig.streaming.metrics.transport.class";
	private static final ConcurrentHashMap<String, IMetricsTransport> instances = new ConcurrentHashMap<String, IMetricsTransport>();
	
	public static boolean hasMetricsTransport(Map props) {
		return props.containsKey(METRICS_TRANSPORT_KEY);
	}
	
	synchronized public static IMetricsTransport getInstance(Map props, ClassLoader cl) {
		// Determine the class from the current PigContext.
		String klazz = (String) props.get(METRICS_TRANSPORT_KEY);
		if (klazz == null) {
			return null;
		}

		IMetricsTransport ret = instances.get(klazz);
		if (ret != null) {
			return ret;
		}

		try {
			Class<? extends IMetricsTransport> cls = 
					(Class<? extends IMetricsTransport>) cl.loadClass(klazz);

			ret = cls.newInstance();
			ret.initialize(props);

			instances.put(klazz, ret);

			return ret;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}		
	}
	
	synchronized public static IMetricsTransport getInstance(PigContext pc) {
		return getInstance(pc.getProperties(), PigContext.getClassLoader());
	}
	
	synchronized public static IMetricsTransport getInstance() {
		if (instances.size() == 0) {
			return null;
		}
		return (IMetricsTransport) (instances.values().toArray())[0];
	}
}
