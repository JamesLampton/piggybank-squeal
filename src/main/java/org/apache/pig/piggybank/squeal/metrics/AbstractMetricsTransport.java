package org.apache.pig.piggybank.squeal.metrics;

public abstract class AbstractMetricsTransport implements IMetricsTransport {

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
}
