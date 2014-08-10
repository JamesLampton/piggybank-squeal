package org.apache.pig.backend.storm.plans;

import java.io.PrintStream;

import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MapReduceOper;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.NativeMapReduceOper;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROperPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PlanPrinter;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.plan.DepthFirstWalker;
import org.apache.pig.impl.plan.VisitorException;

public class SPrinter extends SOpPlanVisitor {
	private boolean isVerbose;
	private PrintStream mStream;
	private PigContext pc;

	public SPrinter(PrintStream ps, SOperPlan plan, PigContext pc) {
		super(plan, new DepthFirstWalker<StormOper, SOperPlan>(plan));
        this.pc = pc;
		mStream = ps;
        mStream.println("#--------------------------------------------------");
        mStream.println("# Storm Topology Plan                              ");
        mStream.println("#--------------------------------------------------");
	}

    public void setVerbose(boolean verbose) {
        isVerbose = verbose;
    }

    @Override
    public void visitSOp(StormOper sop) throws VisitorException {
        mStream.println("Storm node " + sop.getOperatorKey().toString() + " type: " + sop.getType() + " alias: " + sop.name() + " parallel: " + sop.getParallelismHint());
        if (sop.getType() == StormOper.OpType.BASIC_PERSIST || sop.getType() == StormOper.OpType.COMBINE_PERSIST) {
        	mStream.println("Backing Store: " + sop.getStateFactoryOpts(pc));
        	if (sop.getWindowOptions() != null) {
        		mStream.println("Window options: " + sop.getWindowOptions());
        	}
        }
        if (sop.plan != null) {
          PlanPrinter<PhysicalOperator, PhysicalPlan> printer = new PlanPrinter<PhysicalOperator, PhysicalPlan>(sop.plan, mStream);
          printer.setVerbose(isVerbose);
          printer.visit();
          mStream.println("\n--------");        	
        }
        mStream.println("----------------");
        mStream.println("");
    }
}
