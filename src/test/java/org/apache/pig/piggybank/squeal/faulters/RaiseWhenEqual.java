package org.apache.pig.piggybank.squeal.faulters;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

public class RaiseWhenEqual extends EvalFunc<String> {

	@Override
	public String exec(Tuple input) throws IOException {
		if (input.get(0).toString().equals(input.get(1).toString())) {
			throw new RuntimeException("TRIGGERED EQUAL: " + input.get(0).toString() + " == " + input.get(1).toString());
		}
		return "";
	}
}
