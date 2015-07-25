package org.apache.pig.piggybank.evaluation;

import java.io.IOException;

import org.apache.commons.math3.distribution.ExponentialDistribution;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

public class TestDelay extends EvalFunc<Double> {

	private double mean;
	private ExponentialDistribution dist;

	public TestDelay() {
		this("1.");
	}
	
	public TestDelay(String mean) {
		this.mean = Double.parseDouble(mean);
	}


	@Override
	public Double exec(Tuple input) throws IOException {
		if (dist == null) {
			dist = new ExponentialDistribution(mean);
		}
		
		double sleep_time = dist.sample();
		
		try {
			// FIXME: Make this more specific?
			Thread.sleep((long) sleep_time);
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
		
		return sleep_time;
	}

}
