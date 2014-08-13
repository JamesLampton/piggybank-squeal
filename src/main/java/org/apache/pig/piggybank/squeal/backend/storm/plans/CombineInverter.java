package org.apache.pig.piggybank.squeal.backend.storm.plans;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;

import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.PigException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POProject;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POUserFunc;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POForEach;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLocalRearrange;
import org.apache.pig.impl.plan.PlanException;
import org.apache.pig.impl.util.MultiMap;
import org.apache.pig.piggybank.squeal.AlgebraicInverse;

public class CombineInverter {
	private PhysicalPlan plan;
	private MultiMap<PhysicalOperator, PhysicalOperator> opmap;

	public CombineInverter(PhysicalPlan plan) {
		this.plan = plan;
	}
	
	public MultiMap<PhysicalOperator, PhysicalOperator> getLastOpMap() {
		return opmap;
	}
	
	public AlgebraicInverse getHelper(EvalFunc func) throws PlanException {
		// Find the base name.
		String[] components = func.getClass().getName().split("[.]");
		String helper_name = "org.apache.pig.piggybank.squeal.builtin" + components[components.length - 1];
		
		// Attempt to instantiate the class.
		Class helper;
		try {
			helper = func.getClass().getClassLoader().loadClass(helper_name);
			
			// Instantiate the helper.
			AlgebraicInverse instance = (AlgebraicInverse) helper.getConstructor().newInstance();

			
		} catch (ClassNotFoundException e) {
			return null;
		} catch (Exception e) {
			int errCode = 2019;
			String msg = "Unable to instantiate helper class: " + helper_name;
			throw new PlanException(msg, errCode, PigException.BUG);
		}
		
		
		return null;
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

			POUserFunc func_leaf = (POUserFunc)leaf;
			try {
				// We need to patch the funcSpec for the function.
				Class<? extends POUserFunc> klazz = func_leaf.getClass();
				
				Field origFSpec = klazz.getField("origFSpec");
				origFSpec.setAccessible(true);
				
				Method instantiateFunction = klazz.getMethod("instantiateFunc", origFSpec.get(func_leaf).getClass());
				instantiateFunction.setAccessible(true);
				
				instantiateFunction.invoke(func_leaf, origFSpec.get(func_leaf));
				
				Field func_field = klazz.getField("func");
				func_field.setAccessible(true);
				
				Field funcSpec = klazz.getField("funcSpec");
				funcSpec.setAccessible(true);
				
				EvalFunc func = (EvalFunc) func_field.get(func_leaf);
				AlgebraicInverse helper;
				
				if (AlgebraicInverse.class.isAssignableFrom(func.getClass())) {
					funcSpec.set(func_leaf, new FuncSpec(((AlgebraicInverse) func).getInitialInverse()));
				} else if ((helper = this.getHelper(func)) != null) {
					// If there is a helper function in ...piggybank.squeal.builtin, use it.
					funcSpec.set(func_leaf, new FuncSpec(helper.getInitialInverse()));
				} else {
					// Replace the whole function with a wrapper.
					// TODO: Have to replace the other parts of the combiner as well.
				}
				
				/* func.setAlgebraicFunction(type);
				 * 
				 * Patch for POUserFunc:
				 * 
				 * +        case INITIALNEG:
				 * +        	funcSpec = new FuncSpec(getInitialNeg());
				 * +        	break;
				 * 
				 * +    public String getInitialNeg() throws ExecException {
				 * +        instantiateFunc(origFSpec);
				 * +        if (func instanceof AlgebraicInverse && ((AlgebraicInverse) func).getInitialInverse() != null) {
				 * +            return ((AlgebraicInverse) func).getInitialInverse();
				 * +        } else {
				 * +            int errCode = 2072;
				 * +            String msg = "Unable to retrieve inverse for algebraic function: " + func.getClass().getName();
				 * +            throw new ExecException(msg, errCode, PigException.BUG);
				 * +        }
				 * +    }
				 * 
				 */
			} catch (Exception e) {
				int errCode = 2075;
				String msg = "Could not set algebraic function type.";
				throw new PlanException(msg, errCode, PigException.BUG, e);
			}
		}
		
		return ret;
	}
}
