package org.apache.pig.piggybank.squeal.backend.storm.state;

import org.apache.pig.piggybank.squeal.flexy.components.IMapState;
import org.apache.pig.piggybank.squeal.flexy.components.IRunContext;
import org.apache.pig.piggybank.squeal.flexy.components.IStateFactory;

import storm.trident.state.StateFactory;

public class TridentStateWrapper implements IStateFactory {

	public TridentStateWrapper(StateFactory stateFactory) {
		// TODO Auto-generated constructor stub
	}

	@Override
	public IMapState makeState(IRunContext context) {
		// TODO Auto-generated method stub
		return null;
	}

}
