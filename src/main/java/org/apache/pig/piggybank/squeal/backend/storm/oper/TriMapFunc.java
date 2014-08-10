package org.apache.pig.backend.storm.oper;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.HDataType;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MapReducePOStoreImpl;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLocalRearrange;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.util.PlanHelper;
import org.apache.pig.backend.storm.io.StormPOStoreImpl;
import org.apache.pig.backend.storm.plans.CombineInverter;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.NullableTuple;
import org.apache.pig.impl.io.PigNullableWritable;
import org.apache.pig.impl.plan.PlanException;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.util.MultiMap;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

public class TriMapFunc extends StormBaseFunction {

	private static final Tuple DUMMYTUPLE = null;
	private PlanExecutor mapPlanExecutor;
	private PlanExecutor negMapPlanExecutor = null;
	private boolean isLeaf;
	
	@Override
	public void	prepare(Map conf, TridentOperationContext context) {
		super.prepare(conf, context);
		
		// Initialize any stores.
//		System.out.println("TriMapFunc.prepare -- conf: " + conf);
//		System.out.println("TriMapFunc.prepare -- context: " + context.getPartitionIndex());
		
		if (isLeaf) {
			try {
				mapPlanExecutor.setup((String) conf.get("storm.id"), context.getPartitionIndex());
				negMapPlanExecutor.setup((String) conf.get("storm.id"), context.getPartitionIndex());
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
	}
	
	@Override
	public void cleanup() {
		super.cleanup();
		
		if (isLeaf) {
			try {
				mapPlanExecutor.teardown();
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
	}
	
	class PlanExecutor implements Serializable {
		private PhysicalOperator leaf;
		private boolean computeKey;
		private byte mapKeyType;
		private boolean errorInMap;
		private PhysicalOperator root;
		private LinkedList<POStore> stores;
		private AtomicInteger sign;

		protected PlanExecutor(PhysicalPlan plan, byte mapKeyType, PhysicalOperator root, int sign) {
			this.root = root;
			this.sign = new AtomicInteger(sign);
						
			leaf = plan.getLeaves().get(0);
			if (leaf.getClass().isAssignableFrom(POStore.class)) {
				if (!isLeaf) {
					// We need to actually peel the POStore off.
					leaf = plan.getPredecessors(leaf).get(0);
				}
			} else {
				computeKey = true;
			}
			this.mapKeyType = mapKeyType;
			
			try {
				stores = PlanHelper.getPhysicalOperators(plan, POStore.class);
			} catch (VisitorException e) {
				throw new RuntimeException(e);
			}
		}
		
		public void teardown() throws IOException {
			for (POStore store : stores) {
				store.tearDown();
			}
		}

		public void setup(String stormId, int partitionIndex) throws IOException {
			for (POStore store : stores) {
				StormPOStoreImpl impl = new StormPOStoreImpl(stormId, partitionIndex, sign);
				store.setStoreImpl(impl);
				store.setUp();
			}
		}

		public void execute(TridentTuple tuple, TridentCollector collector, Integer tive) {
			// Bind the tuple.
			root.attachInput((Tuple) ((PigNullableWritable) tuple.get(1)).getValueAsPigType());
			
			// And pull the results from the leaf.
			try {
				runPipeLine(collector, tive);
			} catch (ExecException e) {
				throw new RuntimeException(e);
			}
		}
		
	    public void collect(TridentCollector collector, Tuple tuple, Integer tive) throws ExecException {
//	    	System.out.println("Map collect: " + tuple + " mapKeyType: " + mapKeyType);

	    	if (computeKey) {
	    		Byte index = (Byte) tuple.get(0);
	    		PigNullableWritable key =
	    				HDataType.getWritableComparableTypes(tuple.get(1), mapKeyType);
	    		NullableTuple val = new NullableTuple((Tuple)tuple.get(2));

	    		// Both the key and the value need the index.  The key needs it so
	    		// that it can be sorted on the index in addition to the key
	    		// value.  The value needs it so that POPackage can properly
	    		// assign the tuple to its slot in the projection.
	    		key.setIndex(index);
	    		val.setIndex(index);

//	    		System.out.println("Emit k: " + key + " -- v: " + val);
	    		collector.emit(new Values(key, val, tive));    		
	    	} else {
	    		collector.emit(new Values(null, new NullableTuple(tuple), tive));
	    	}
	    }
		
		void runPipeLine(TridentCollector collector, Integer tive) throws ExecException {
			while(true){
	            Result res = leaf.getNextTuple();
	            if(res.returnStatus==POStatus.STATUS_OK){
	            	
	                collect(collector, (Tuple)res.result, tive);
	                continue;
	            }
	            
	            if(res.returnStatus==POStatus.STATUS_EOP) {
	                return;
	            }
	            
	            if(res.returnStatus==POStatus.STATUS_NULL)
	                continue;
	            
	            if(res.returnStatus==POStatus.STATUS_ERR){
	                // remember that we had an issue so that in 
	                // close() we can do the right thing
	                errorInMap = true;
	                // if there is an errmessage use it
	                String errMsg;
	                if(res.result != null) {
	                    errMsg = "Received Error while " +
	                    "processing the map plan: " + res.result;
	                } else {
	                    errMsg = "Received Error while " +
	                    "processing the map plan.";
	                }
	                    
	                int errCode = 2055;
	                ExecException ee = new ExecException(errMsg, errCode, PigException.BUG);
	                throw new RuntimeException(ee);
	            }
	        }
		}
	}

	public TriMapFunc(PigContext pc, PhysicalPlan physicalPlan, byte mapKeyType, boolean isCombined, PhysicalOperator activeRoot, boolean isLeaf) {
		super(pc);
		
		// Set this variable to determine if the store leaves are removed.
		this.isLeaf = isLeaf;
		
		// Pull out the active root and get the predecessor.
		List<PhysicalOperator> roots = physicalPlan.getSuccessors(activeRoot);
		if (roots.size() > 1) {
			throw new RuntimeException("Expected only a single successor from the active root.");
		}
//		System.out.println("TriMapFunc: " + activeRoot + " -> " + roots.get(0));
		activeRoot = roots.get(0);
		
		// Remove the root
		List<PhysicalOperator> remove_roots = new ArrayList<PhysicalOperator>(physicalPlan.getRoots());
		for (PhysicalOperator pl : remove_roots) {
			// Remove the root so it can never be reached.
			physicalPlan.remove(pl);
		}

		mapPlanExecutor = new PlanExecutor(physicalPlan, mapKeyType, activeRoot, 1);
		
		PhysicalPlan negClone;
		MultiMap<PhysicalOperator, PhysicalOperator> opmap;
		// See if this plan requires a modified negative pipeline.
		if (isCombined) {
			CombineInverter ci = new CombineInverter(physicalPlan);
			try {
				negClone = ci.getInverse();
				opmap = ci.getLastOpMap();
			} catch (Exception e) {
				throw new RuntimeException(e);
			}			
		} else {
			// Otherwise, create the negativePipeline anyway because of a-sync operators.
			opmap = new MultiMap<PhysicalOperator, PhysicalOperator>();			
			physicalPlan.setOpMap(opmap);
			try {
				negClone = physicalPlan.clone();
			} catch (CloneNotSupportedException e) {
				throw new RuntimeException(e);
			}
			physicalPlan.resetOpMap();
		}
		// track activeRoot through the clone.
		PhysicalOperator negActiveRoot = opmap.get(activeRoot).get(0);
		negMapPlanExecutor  = new PlanExecutor(negClone, mapKeyType, negActiveRoot, -1);
	}
	
	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
//		System.out.println("Map: " + tuple + " activeRoot: " + mapPlanExecutor.root + " leaf: " + mapPlanExecutor.leaf);
		
		// Determine if the tuple is positive or negative
		Integer tive = tuple.getInteger(2);
		
		if (tive < 0) {
			negMapPlanExecutor.execute(tuple, collector, tive);
		} else {
			mapPlanExecutor.execute(tuple, collector, tive);
		}
	}

	static public class MakeKeyRawValue extends BaseFunction {
		@Override
		public void execute(TridentTuple tuple, TridentCollector collector) {
			// Re-wrap the object with a new PigNullableWritable.
			
			PigNullableWritable t = (PigNullableWritable) tuple.get(0);
			Object o = t.getValueAsPigType();
			
			try {
				PigNullableWritable raw = HDataType.getWritableComparableTypes(o, HDataType.findTypeFromNullableWritable(t));
				collector.emit(new Values(raw));
			} catch (ExecException e) {
				throw new RuntimeException(e);
			}
		}		
	}
	
	static public class Copy extends BaseFunction {
		@Override
		public void execute(TridentTuple tuple, TridentCollector collector) {
			collector.emit(new ArrayList<Object>(tuple.getValues()));
		}		
	}
}
