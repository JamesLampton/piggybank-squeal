package org.apache.pig.piggybank.evaluation;

import java.io.IOException;
import java.util.Random;

import org.apache.pig.EvalFunc;
import org.apache.pig.builtin.OutputSchema;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

@OutputSchema("expand:bag{t:tuple(rand:int)}")
public class TestGenerateBag extends EvalFunc<DataBag> {

	private double mean;
	private double std;
	Random dist;
	Random dist2;

	public TestGenerateBag() {
		this("0.", "1.");
	}
	
	public TestGenerateBag(String mean, String std) {
		this.mean = Double.parseDouble(mean);
		this.std = Double.parseDouble(std);
	}
	
	@Override
	public DataBag exec(Tuple input) throws IOException {
		if (dist == null) {
			dist = new Random();
			dist2 = new Random();
		}
		
		// Determine the bag size.
		double count = dist.nextGaussian()*std + mean;
		DataBag bag = BagFactory.getInstance().newDefaultBag();
		for (int i = 0; i < count; i++) {
			Tuple t = TupleFactory.getInstance().newTuple(1);
			t.append(dist2.nextInt());
			bag.add(t);
		}
		
		return bag;
	}

	
}
