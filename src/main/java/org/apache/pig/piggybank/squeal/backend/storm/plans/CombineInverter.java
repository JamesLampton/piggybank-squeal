package org.apache.pig.backend.storm.plans;

import java.util.List;

import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POProject;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POUserFunc;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POForEach;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLocalRearrange;
import org.apache.pig.impl.plan.PlanException;
import org.apache.pig.impl.util.MultiMap;

public class CombineInverter {
	private PhysicalPlan plan;
	private MultiMap<PhysicalOperator, PhysicalOperator> opmap;

	public CombineInverter(PhysicalPlan plan) {
		this.plan = plan;
	}
	
	public MultiMap<PhysicalOperator, PhysicalOperator> getLastOpMap() {
		return opmap;
	}
	
	public PhysicalPlan getInverse() throws CloneNotSupportedException, PlanException {
		opmap = new MultiMap<PhysicalOperator, PhysicalOperator>();
		plan.setOpMap(opmap);
		
		// Clone the plan.
		PhysicalPlan ret = plan.clone();
		
		plan.resetOpMap();

		// Find the ForEach with combiner UDFs
		PhysicalOperator l = ret.getLeaves().get(0);
		if (l == null || !(l instanceof POLocalRearrange)) {
			// This is really a bug...
			throw new RuntimeException("Expected leaf to be of type POLocalRearrange.");
		}
		POForEach foreach = (POForEach) ret.getPredecessors(l).get(0);
				
		// Cycle through the UDFs and swap them with InitNeg.
		// Taken from CombinerOptimizer.
		byte type = POUserFunc.INITIALNEG;
		for(PhysicalPlan p : foreach.getInputPlans()){
			List<PhysicalOperator> leaves = p.getLeaves();
			if (leaves == null || leaves.size() != 1) {
				int errCode = 2019;
				String msg = "Expected to find plan with single leaf. Found " + leaves.size() + " leaves.";
				throw new PlanException(msg, errCode, PigException.BUG);
			}

			PhysicalOperator leaf = leaves.get(0);
			if(leaf instanceof POProject){
				continue;
			}
			if (!(leaf instanceof POUserFunc)) {
				int errCode = 2020;
				String msg = "Expected to find plan with UDF or project leaf. Found " + leaf.getClass().getSimpleName();
				throw new PlanException(msg, errCode, PigException.BUG);
			}

			POUserFunc func = (POUserFunc)leaf;
			try {
				func.setAlgebraicFunction(type);
			} catch (ExecException e) {
				int errCode = 2075;
				String msg = "Could not set algebraic function type.";
				throw new PlanException(msg, errCode, PigException.BUG, e);
			}
		}
		
		return ret;
	}
}
