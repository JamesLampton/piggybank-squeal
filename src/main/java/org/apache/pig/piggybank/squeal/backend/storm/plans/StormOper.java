package org.apache.pig.backend.storm.plans;

import java.util.List;

import org.apache.pig.LoadFunc;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.storm.io.SpoutWrapper;
import org.apache.pig.backend.storm.state.StateWrapper;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.plan.Operator;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.VisitorException;

import backtype.storm.topology.IRichSpout;
import backtype.storm.tuple.Fields;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.Function;
import storm.trident.state.StateFactory;
import storm.trident.testing.LRUMemoryMapState;

public class StormOper extends Operator<SOpPlanVisitor> {

	public enum OpType {
		MAP, COMBINE_PERSIST, BASIC_PERSIST, REDUCE_DELTA, SPOUT
	}

	private OpType t;
	private String alias;

	public StormOper(OperatorKey k, OpType t, String alias) {
		super(k);
		this.t = t;
		this.alias = alias;
	}

	public byte mapKeyType;

	public OpType getType() {
		return t;
	}

	PhysicalPlan plan;

	public void setPlan(PhysicalPlan plan) {
		this.plan = plan;
	}

	public PhysicalPlan getPlan() {
		return this.plan;
	}

	IRichSpout spout;
	public boolean isCombined;
	private String windowOpts;
	private int parallelismHint = 0;
	private String tupleConverterKlass;
	private boolean shuffleBefore;

	public void setSpout(IRichSpout spout) {
		this.spout = spout;
	}

	public IRichSpout getLoadFunc() {
		return this.spout;
	}

	@Override
	public void visit(SOpPlanVisitor v) throws VisitorException {
		// TODO Auto-generated method stub
		v.visitSOp(this);
	}

	@Override
	public boolean supportsMultipleInputs() {
		return true;
	}

	@Override
	public boolean supportsMultipleOutputs() {
		return true;
	}

	@Override
	public String name() {
		return alias;
	}
	
	static public StateFactory getStateFactory(PigContext pc, String alias) {
		// Pull the leaf's name and look for storage options.
		String store_opts = pc.getProperties().getProperty(alias + "_store_opts");
//		System.out.println("getStateFactory: " + alias + " args: " + store_opts);
		return new StateWrapper(store_opts).getStateFactory();
	}
	
	public StateFactory getStateFactory(PigContext pc) {
		return getStateFactory(pc, alias);
	}
	
	public String getStateFactoryOpts(PigContext pc) {
		if (pc == null) {
			return "unknown";
		}
		return pc.getProperties().getProperty(alias + "_store_opts");
	}

	public Fields getOutputFields() {
		String field_pre = getOperatorKey().toString();
		if (t == OpType.BASIC_PERSIST || t == OpType.COMBINE_PERSIST) {
			return new Fields(field_pre + "_vl");
		}
		return new Fields(field_pre + "_k", field_pre + "_v", field_pre + "_tive");
	}

	public boolean getIsCombined() {
		return isCombined;
	}

	public void setWindowOptions(String opts) {
		windowOpts = opts;		
	}

	public String getWindowOptions() {
		return windowOpts;
	}

	public void setParallelismHint(int parallelismHint) {
		this.parallelismHint = parallelismHint;
	}
	public int getParallelismHint() {
		return parallelismHint;
	}

	public static String getWindowOpts(PigContext pc, String alias) {
		return pc.getProperties().getProperty(alias + "_window_opts");
	}

	public void setTupleConverter(Class<? extends BaseFunction> tupleConverter) {
		this.tupleConverterKlass = tupleConverter.getName();
	}

	public Function getTupleConverter() {
		try {
			return (Function) Class.forName(this.tupleConverterKlass).newInstance();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public void shuffleBefore(boolean b) {
		this.shuffleBefore = b;
	}

	public boolean getShuffleBefore() {
		return this.shuffleBefore;
	}
}
