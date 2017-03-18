package org.apache.pig.piggybank.squeal.flexy.components.impl;

import java.util.List;

import org.apache.pig.piggybank.squeal.flexy.components.IFlexyTuple;
import org.apache.pig.piggybank.squeal.flexy.model.FFields;

import backtype.storm.tuple.Fields;

public class FlexyTupleFactory {

	public static FlexyTupleFactory newFreshOutputFactory(FFields inputSchema) {
		// TODO Auto-generated method stub with exception
		throw new RuntimeException("Not implemented");
	}

	public IFlexyTuple create(IFlexyTuple tup) {
		// TODO Auto-generated method stub with exception
		throw new RuntimeException("Not implemented");
	}

	public IFlexyTuple create(IFlexyTuple parent, List<Object> values) {
		// TODO Auto-generated method stub with exception
		throw new RuntimeException("Not implemented");
	}

	public IFlexyTuple create(List<Object> values) {
		// TODO Auto-generated method stub with exception
		throw new RuntimeException("Not implemented");
	}

	public static FlexyTupleFactory newProjectionFactory(FFields inputFields) {
		// TODO Auto-generated method stub with exception
		throw new RuntimeException("Not implemented");
	}

	public static FlexyTupleFactory newOperationOutputFactory(
			FlexyTupleFactory parent_tf, FFields appendOutputFields) {
		// TODO Auto-generated method stub with exception
		throw new RuntimeException("Not implemented");
	}

	public static IFlexyTuple newTuple(List<String> list, List<Object> values) {
		// TODO Auto-generated method stub with exception
		throw new RuntimeException("Not implemented");
	}

}
