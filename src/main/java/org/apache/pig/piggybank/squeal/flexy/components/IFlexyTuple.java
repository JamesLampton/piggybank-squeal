package org.apache.pig.piggybank.squeal.flexy.components;

import java.util.Collection;
import java.util.List;

import org.apache.hadoop.io.Writable;
import org.apache.pig.piggybank.squeal.backend.storm.state.MapIdxWritable;

import backtype.storm.tuple.Fields;

public interface IFlexyTuple extends Collection {

	Object get(int i);
	int getInteger(int i);
	int size();
	List<Object> getValues();
	Writable getValueByField(String string);
	byte[] getBinary(int i);
	long getLong(int i);
	boolean getBoolean(int i);
	Object select(Fields gr_fields);

}
