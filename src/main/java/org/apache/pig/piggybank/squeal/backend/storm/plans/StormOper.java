/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.pig.piggybank.squeal.backend.storm.plans;

import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.piggybank.squeal.backend.storm.state.StateWrapper;
import org.apache.pig.piggybank.squeal.flexy.components.IFunction;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.plan.Operator;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.VisitorException;

import backtype.storm.topology.IRichSpout;
import backtype.storm.tuple.Fields;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.Function;
import storm.trident.state.StateFactory;

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

	public void setTupleConverter(Class<? extends IFunction> tupleConverter) {
		this.tupleConverterKlass = tupleConverter.getName();
	}

	public IFunction getTupleConverter() {
		try {
			return (IFunction) Class.forName(this.tupleConverterKlass).newInstance();
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
