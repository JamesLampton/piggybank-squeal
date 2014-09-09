package org.apache.pig.piggybank.squeal.metrics;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.pig.impl.PigContext;

public class MetricsTransportFactory {
	public static final String METRICS_TRANSPORT_KEY = "pig.streaming.metrics.transport.class";
	private static final ConcurrentHashMap<String, IMetricsTransport> instances = new ConcurrentHashMap<String, IMetricsTransport>();
	
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
