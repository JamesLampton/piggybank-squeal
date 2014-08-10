package org.apache.pig.piggybank.squeal;

import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MapReduceOper;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLoad;
import org.apache.pig.impl.PigContext;

public class MonkeyPatch {
	
	public static final byte POUserFuncINITIALNEG = -1;

	public static void PigContextRefreshEngine(PigContext pc) {
		// TODO Auto-generated method stub
//		pc.refreshExecutionEngine();
	}

	public static void POLoadSetAlias(POLoad nopLoad, String alias) {
		// TODO Auto-generated method stub
//		nopLoad.setAlias(load.getAlias());
	}

	public static void MapReduceOperSetRequestedParallelism(
			MapReduceOper state_mr, int requestedParallelism) {
//		state_mr.setRequestedParallelism(mr.getRequestedParallelism());
		// TODO Auto-generated method stub
		
	}

}
