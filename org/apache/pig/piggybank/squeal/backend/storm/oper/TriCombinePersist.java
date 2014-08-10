package org.apache.pig.backend.storm.oper;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.HDataType;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POPackage;
import org.apache.pig.backend.storm.state.CombineTupleWritable;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.io.NullableTuple;
import org.apache.pig.impl.io.NullableUnknownWritable;
import org.apache.pig.impl.io.PigNullableWritable;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.CombinerAggregator;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

public class TriCombinePersist implements CombinerAggregator<CombineTupleWritable> {

	private PhysicalPlan combinePlan;
	private POPackage pack;
	private byte keyType;
	private PhysicalOperator[] roots;
	private PhysicalOperator leaf;
	private final static Tuple DUMMYTUPLE = null;
	private final static PhysicalOperator[] DUMMYROOTARR = {};

	public TriCombinePersist(POPackage pack, PhysicalPlan plan, byte mapKeyType) {
		combinePlan = plan;
		this.pack = pack;
		keyType = mapKeyType;
		roots = plan.getRoots().toArray(DUMMYROOTARR);
		leaf = plan.getLeaves().get(0);
	}
	
	CombineTupleWritable runCombine(PigNullableWritable inKey, List<NullableTuple> tuplist) {
		pack.attachInput(inKey, tuplist.iterator());
		ArrayList<CombineTupleWritable> ret = new ArrayList<CombineTupleWritable>();
		
		try {
            Result res = pack.getNextTuple();
            if(res.returnStatus==POStatus.STATUS_OK){
                Tuple packRes = (Tuple)res.result;
                                
                if(combinePlan.isEmpty()){
                	CombineTupleWritable tw = new CombineTupleWritable(new Writable[] {new NullableUnknownWritable(null), packRes});
                	
                	ret.add(new CombineTupleWritable(new Writable[] {new NullableUnknownWritable(null), packRes}));
                }
                
                for (int i = 0; i < roots.length; i++) {
                    roots[i].attachInput(packRes);
                }
                while(true){
                    Result redRes = leaf.getNextTuple();
                    
                    if(redRes.returnStatus==POStatus.STATUS_OK){
                        Tuple tuple = (Tuple)redRes.result;
                        Byte index = (Byte)tuple.get(0);
                        PigNullableWritable outKey =
                            HDataType.getWritableComparableTypes(tuple.get(1), this.keyType);
                        NullableTuple val =
                            new NullableTuple((Tuple)tuple.get(2));
                        // Both the key and the value need the index.  The key needs it so
                        // that it can be sorted on the index in addition to the key
                        // value.  The value needs it so that POPackage can properly
                        // assign the tuple to its slot in the projection.
                        outKey.setIndex(index);
                        val.setIndex(index);

                        // FIXME: When would the key be different from the input key?
                        ret.add(new CombineTupleWritable(new Writable[] {outKey, val}));
                        continue;
                    }
                    
                    if(redRes.returnStatus==POStatus.STATUS_EOP) {
                        break;
                    }
                    
                    if(redRes.returnStatus==POStatus.STATUS_NULL) {
                        continue;
                    }
                    
                    if(redRes.returnStatus==POStatus.STATUS_ERR){
                        int errCode = 2090;
                        String msg = "Received Error while " +
                        "processing the combine plan.";
                        if(redRes.result != null) {
                            msg += redRes.result;
                        }
                        throw new ExecException(msg, errCode, PigException.BUG);
                    }
                }
            }
                        
            if(res.returnStatus==POStatus.STATUS_ERR){
                int errCode = 2091;
                String msg = "Packaging error while processing group.";
                throw new ExecException(msg, errCode, PigException.BUG);
            }
            
            if(res.returnStatus==POStatus.STATUS_EOP) {
            	// FIXME: ???
//                return true;
            }
                
//            return false;
//            System.out.println("Combine OUT: " + ret);
            assert(ret.size() <= 1);
            return ret.get(0);
            
        } catch (ExecException e) {
            throw new RuntimeException(e);
        }
		
//		return null;
	}
	
	
	@Override
	public CombineTupleWritable init(TridentTuple tri_tuple) {
		
//		System.out.println("TriCombinePersist.init(): " + tri_tuple);
		
		ArrayList<NullableTuple> tuplist = new ArrayList<NullableTuple>();
		tuplist.add((NullableTuple) tri_tuple.get(1));
		
		return runCombine((PigNullableWritable) tri_tuple.get(0), tuplist);
	}

	@Override
	public CombineTupleWritable combine(CombineTupleWritable val1, CombineTupleWritable val2) {
		CombineTupleWritable ret = null;
		
//		System.out.println("TriCombine --  v1: " + val1 + "  v2: " + val2);
		
		PigNullableWritable inKey = null;
		ArrayList<NullableTuple> tuplist = new ArrayList<NullableTuple>();

		if (val1 != null) {
			if (inKey == null) {
				// TODO: Check and make sure they're the same?
				inKey = (PigNullableWritable) val1.get(0);					
			}
//			System.out.println("v1: " + val1.get(0) + " " + val1.get(1));
			tuplist.add((NullableTuple) val1.get(1));
		}

		if (val2 != null) {
			if (inKey == null) {
				// TODO: Check and make sure they're the same?
				inKey = (PigNullableWritable) val2.get(0);
			}
			tuplist.add((NullableTuple) val2.get(1));
		}

		if (inKey != null) {
			ret = runCombine(inKey, tuplist);
		}
		
//		System.out.println("OUT: " + ret);
		return ret;
	}

	@Override
	public CombineTupleWritable zero() {
		return null;
	}

	public static class StateClean extends BaseFunction {
		@Override
		public void execute(TridentTuple tuple, TridentCollector collector) {
			Values stuff = (Values) tuple.get(1);
			collector.emit(new Values(stuff.get(1)));
		}
	}
	
	static public List<NullableTuple> getTuples(CombineTupleWritable state) {
		List<NullableTuple>  tuples = new ArrayList<NullableTuple>();
		tuples.add((NullableTuple) state.get(1));
		return tuples;
	}
}
