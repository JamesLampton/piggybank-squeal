package org.apache.pig.piggybank.squeal.flexy.components;

import java.util.Map;

import org.apache.pig.piggybank.squeal.flexy.model.FFields;
import org.apache.pig.piggybank.squeal.flexy.model.FStream;

public interface IRunContext {

	int getThisTaskId();

	String getThisComponentId();

	int getThisTaskIndex();

	String getStormId();

	int getPartitionIndex();

	Object get(String key);

	String getExposedName(FStream cur);

	Map getStormConf();

	FFields getInputSchema();

	void runWaitStrategy(int emptyStreak);

}
