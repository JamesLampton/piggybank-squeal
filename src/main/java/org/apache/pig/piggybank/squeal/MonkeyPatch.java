package org.apache.pig.piggybank.squeal;

import java.lang.reflect.Field;
import java.util.List;

import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MapReduceOper;
import org.apache.pig.impl.PigContext;

import com.sun.tools.attach.VirtualMachine;

import storm.trident.operation.TridentOperationContext;
import backtype.storm.messaging.netty.Client;
import backtype.storm.task.TopologyContext;

public class MonkeyPatch {
	
	public static void PigContextRefreshEngine(PigContext pc) {
		/* Patch for PigContext:
		 * 
		 * +    public void refreshExecutionEngine() {
         * +    	executionEngine = execType.getExecutionEngine(this);
         * +    }
		 */
		
		// We're going to use reflection to hack this one out.
		try {
			Class<? extends PigContext> klazz = pc.getClass();
			Field executionEngine = klazz.getDeclaredField("executionEngine");
			executionEngine.setAccessible(true);
			executionEngine.set(pc, pc.getExecType().getExecutionEngine(pc));
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public static void MapReduceOperSetRequestedParallelism(
			MapReduceOper state_mr, int requestedParallelism) {
		/* Patch for MapReduceOper:
		 * 
		 * +    public void setRequestedParallelism(int rp) {
		 * +        requestedParallelism = rp;
		 * +    }
		 */
		
		// We're going to use reflection to hack this one out.
		try {
			Class<? extends MapReduceOper> klazz = state_mr.getClass();
			Field f_rp = klazz.getDeclaredField("requestedParallelism");
			f_rp.setAccessible(true);
			f_rp.set(state_mr, requestedParallelism);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}		
	}
	
	public static TopologyContext getTopologyContext(TridentOperationContext tri_context) {
		try {
			Class<? extends TridentOperationContext> klazz = tri_context.getClass();
			Field f_other_context = klazz.getDeclaredField("_topoContext");
			f_other_context.setAccessible(true);;
			return (TopologyContext) f_other_context.get(tri_context);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
}
