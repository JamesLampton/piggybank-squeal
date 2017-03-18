package org.apache.pig.piggybank.squeal.flexy.components;

import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public interface IOutputCollector {

	void emit(String exposedName, Tuple anchor, Values values);

}
