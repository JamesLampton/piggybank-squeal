package org.apache.pig.piggybank.squeal.flexy.components;

import java.util.List;

public interface ICollector {

	void emit(List<Object> values);

	void reportError(Throwable t);

}
