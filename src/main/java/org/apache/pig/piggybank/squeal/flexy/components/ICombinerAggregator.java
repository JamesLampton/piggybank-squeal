package org.apache.pig.piggybank.squeal.flexy.components;

import java.io.Serializable;

public interface ICombinerAggregator <T> extends Serializable {
	T init(IFlexyTuple tuple);
    T combine(T val1, T val2);
    T zero();
}
