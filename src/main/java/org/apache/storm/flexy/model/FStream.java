package org.apache.storm.flexy.model;

import storm.trident.operation.Function;
import backtype.storm.tuple.Fields;

public class FStream {

	public FStream name(String simpleName) {
		// TODO Auto-generated method stub
		return null;
	}

	public void parallelismHint(int parallelismHint) {
		// TODO Auto-generated method stub
		
	}

	public Fields getOutputFields() {
		// TODO Auto-generated method stub
		return null;
	}

	public FStream each(Fields outputFields, Function tupleConverter,
			Fields output_fields) {
		// TODO Auto-generated method stub
		return null;
	}

	public FStream project(Fields output_fields) {
		// TODO Auto-generated method stub
		return null;
	}

	public FStream shuffle() {
		// TODO Auto-generated method stub
		return null;
	}

	public GroupedFStream groupBy(Fields group_key) {
		// TODO Auto-generated method stub
		return null;
	}

}
