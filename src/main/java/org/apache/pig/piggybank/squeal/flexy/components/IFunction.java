package org.apache.pig.piggybank.squeal.flexy.components;

public interface IFunction {
	public void prepare(IRunContext context);
	public void execute(IFlexyTuple tuple, ICollector collector);
	public void cleanup();
}
