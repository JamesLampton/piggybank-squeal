package org.apache.pig.backend.storm.oper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.HDataType;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POPackage;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.util.PlanHelper;
import org.apache.pig.backend.storm.io.StormPOStoreImpl;
import org.apache.pig.backend.storm.oper.CombineWrapper.CombineWrapperState;
import org.apache.pig.backend.storm.state.CombineTupleWritable;
import org.apache.pig.backend.storm.state.IPigIdxState;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.NullableTuple;
import org.apache.pig.impl.io.PigNullableWritable;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.util.Pair;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

public class TriReduce extends StormBaseFunction {

	private PhysicalPlan reducePlan;
	private PhysicalOperator[] roots;
	private PhysicalOperator leaf;
	private POPackage pack;
	private boolean errorInReduce;
	private boolean noNegative;
	private boolean isLeaf;
	private LinkedList<POStore> stores;
	private AtomicInteger sign;
	private final static Tuple DUMMYTUPLE = null;
	private final static PhysicalOperator[] DUMMYROOTARR = {};
	private final static Integer POS = 1;
	private final static Integer NEG = -1;
	
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
	
	@Override
	public void	prepare(Map conf, TridentOperationContext context) {
		super.prepare(conf, context);
		
		// Initialize any stores.
		if (isLeaf) {
			try {
				setup((String) conf.get("storm.id"), context.getPartitionIndex());
				// TODO: -- handle the negative stuff too.
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
	}
	
	public TriReduce(PigContext pc, PhysicalPlan plan, boolean noNegative, boolean isLeaf) {
		super(pc);
		
		this.noNegative = noNegative;
		this.isLeaf = isLeaf;
		this.sign = new AtomicInteger(0);
		
		// We need to trim things from the plan re:GenericMapReduce.java
		reducePlan = plan;
		pack = (POPackage) plan.getRoots().get(0);
		plan.remove(pack);
//		keyType = mapKeyType;
		roots = plan.getRoots().toArray(DUMMYROOTARR);
		
		leaf = plan.getLeaves().get(0);
//		System.out.println("TriReduce roots: " + roots + " leaf: " + leaf + " isEmpty: " + reducePlan.isEmpty());
		if (leaf.getClass().isAssignableFrom(POStore.class)) {
			if (isLeaf) {
				try {
					stores = PlanHelper.getPhysicalOperators(plan, POStore.class);
				} catch (VisitorException e) {
					throw new RuntimeException(e);
				}	
			}
			
			// We need to actually peel the POStore off.
			if (reducePlan.getPredecessors(leaf) != null) {
				leaf = reducePlan.getPredecessors(leaf).get(0);
			} else {
				leaf = null;
			}
		}
	}
	
	class FakeCollector implements TridentCollector {

		private TridentCollector collector;
		
		private Map<Writable, IntWritable> last_res = new HashMap<Writable, IntWritable>();
		private Map<Writable, IntWritable> cur_res = new HashMap<Writable, IntWritable>();

		int state = 0;
		
		public FakeCollector(TridentCollector collector) {
			this.collector = collector;
		}
		
		public void switchToCur() {
			state = 1;
		}
		
		void inc(Map<Writable, IntWritable> m, Tuple v) {
			IntWritable iw = m.get(v);
			if (iw == null) {
				iw = new IntWritable(0);
				m.put(v, iw);
			}
			iw.set(iw.get() + 1);
		}
		
		@Override
		public void emit(List<Object> values) {
//			System.out.println("Emit: " + values.get(1) + " -> " + values.get(1).getClass());
			// Pull the value
			Tuple v = (Tuple) values.get(1);
			
			if (state == 0) {
				inc(last_res, v);
			} else {
				// See if v was in the last_set.
				IntWritable iw = last_res.get(v);
				if (iw == null) {
					// We have a new message.
					inc(cur_res, v);
				} else {
					// Decrement last_res.
					int cur = iw.get() - 1;
					// Remove v from last_res if we can account for all the previous messages.
					if (cur == 0) {
						last_res.remove(v);
					} else {
						iw.set(cur);
					}
				}
			}
		}

		@Override
		public void reportError(Throwable t) {
			collector.reportError(t);
		}
		
		void emitSet(Set<Entry<Writable, IntWritable>> s, Integer msign) {
			for (Entry<Writable, IntWritable>  ent: s) {
				int count = ent.getValue().get();
//				System.err.println("Pos: " + ent);
				for (int i = 0; i < count ; i++) {
					byte t = DataType.findType(ent.getKey());
					try {
						// Emit to the STORE function if we're a leaf.
						if (isLeaf) {
							// Set the sign reference appropriately.
							sign.set(msign);
							// Attach the input to the store function and empty it.
							for (POStore store : stores) {
//								System.out.println("emitSet: " + ent.getKey());
								Tuple tup = (Tuple) ent.getKey();
								store.attachInput(tup);
								store.getNextTuple();
							}
						}
												
						// Emit to the stream.
						collector.emit(new Values(null, HDataType.getWritableComparableTypes(ent.getKey(), t), msign));
					} catch (ExecException e) {
						throw new RuntimeException(e);
					}
				}
			}
		}
		
		// Emit positive and negative messages.
		public void emitValues() {
			// Any values in cur_set go out as "positive" messages.
			emitSet(cur_res.entrySet(), POS);
			
			// Don't emit negative messages.
			if (noNegative) {
				return;
			}
			
			// Any values in last_set go out as "negative" messages.
			emitSet(last_res.entrySet(), NEG);
		}
	}
	
	@Override
	public void execute(TridentTuple tri_tuple, TridentCollector collector) {
//		System.out.println("TriReduce input: " + tri_tuple);
		
		PigNullableWritable key = (PigNullableWritable) tri_tuple.get(0);
		
		CombineWrapperState cw = (CombineWrapperState) tri_tuple.get(1);
		
		for (Pair<List<NullableTuple>, List<NullableTuple>> p : cw.getTupleBatches(null)) {
			
			FakeCollector fc = new FakeCollector(collector);

			try {
				// Calculate the previous values.
				List<NullableTuple> tuples;
				tuples = p.first;
				if (tuples != null) {
					runReduce(key, tuples, fc);
				}

				//		System.out.println("TriReduce |last_input|: " + ((tuples == null) ? 0 : tuples.size()) + " |last_output| : " + fc.last_res.size());
				//		if (tuples != null) {
				//			System.out.println("last_input: " + tuples);
				//		}
				//		System.out.println("last_output: " + fc.last_res);

				// Calculate the current values.
				tuples = p.second;
				fc.switchToCur();
				runReduce(key, tuples, fc);
			} catch (Exception e) {
				throw new RuntimeException("Error processing: " + tri_tuple, e);
			}
			
//			System.out.println("TriReduce |cur_input|: " + tuples.size() + " |cur_output| : " + fc.cur_res.size());

			// Emit positive and negative values.
			fc.emitValues();
		}
	}

	public void runReduce(PigNullableWritable key, List<NullableTuple> tuples, TridentCollector collector) {
		try {
			pack.attachInput(key, tuples.iterator());
			if (pack instanceof POPackage)
			{
				while (true)
				{
					if (processOnePackageOutput(collector))
						break;
				}
			}
			else {
				// join is not optimized, so package will
				// give only one tuple out for the key
				processOnePackageOutput(collector);
			} 
		} catch (ExecException e) {
			throw new RuntimeException(e);
		}
	}
	
	public boolean processOnePackageOutput(TridentCollector collector) throws ExecException  {
        Result res = pack.getNextTuple();
        if(res.returnStatus==POStatus.STATUS_OK){
            Tuple packRes = (Tuple)res.result;
            
            if(leaf == null || reducePlan.isEmpty()){
                collector.emit(new Values(null, packRes));
                return false;
            }
            for (int i = 0; i < roots.length; i++) {
                roots[i].attachInput(packRes);
            }
            runPipeline(leaf, collector);
        }
        
        if(res.returnStatus==POStatus.STATUS_NULL) {
            return false;
        }
        
        if(res.returnStatus==POStatus.STATUS_ERR){
            int errCode = 2093;
            String msg = "Encountered error in package operator while processing group.";
            throw new ExecException(msg, errCode, PigException.BUG);
        }
        
        if(res.returnStatus==POStatus.STATUS_EOP) {
            return true;
        }
            
        return false;
    }
    
    /**
     * @param leaf
     * @param collector 
     * @throws ExecException 
     */
    protected void runPipeline(PhysicalOperator leaf, TridentCollector collector) throws ExecException {
        
        while(true)
        {
            Result redRes = leaf.getNextTuple();
            if(redRes.returnStatus==POStatus.STATUS_OK){
                collector.emit(new Values(null, (Tuple)redRes.result));
                continue;
            }
            
            if(redRes.returnStatus==POStatus.STATUS_EOP) {
                return;
            }
            
            if(redRes.returnStatus==POStatus.STATUS_NULL) {
                continue;
            }
            
            if(redRes.returnStatus==POStatus.STATUS_ERR){
                // remember that we had an issue so that in 
                // close() we can do the right thing
                errorInReduce = true;
                // if there is an errmessage use it
                String msg;
                if(redRes.result != null) {
                    msg = "Received Error while " +
                    "processing the reduce plan: " + redRes.result;
                } else {
                    msg = "Received Error while " +
                    "processing the reduce plan.";
                }
                int errCode = 2090;
                throw new ExecException(msg, errCode, PigException.BUG);
            }
        }
    }
}
