package org.apache.pig.piggybank.squeal.flexy.components;

public interface IStateFactory<T> {

	public IMapState<T> makeState(IRunContext context);

}
