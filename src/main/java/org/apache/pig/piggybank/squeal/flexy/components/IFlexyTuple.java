package org.apache.pig.piggybank.squeal.flexy.components;

import java.util.Collection;
import java.util.List;

import org.apache.hadoop.io.Writable;
import org.apache.pig.piggybank.squeal.backend.storm.state.MapIdxWritable;

public interface IFlexyTuple extends Collection {

	Object get(int i);
	int getInteger(int i);
	int size();
	List<Object> getValues();
	Writable getValueByField(String string);

}
