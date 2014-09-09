package org.apache.pig.piggybank.squeal.metrics;

import java.util.Map;
import java.util.Properties;

import org.apache.pig.impl.PigContext;

public interface IMetricsTransport {
	public void send(Object...objects );
	public void send(byte[] buf);
	public void initialize(PigContext pc);
	public void initialize(Map prop);
}
